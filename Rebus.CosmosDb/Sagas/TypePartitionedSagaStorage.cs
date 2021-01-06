using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Rebus.Exceptions;
using Rebus.Pipeline;
using Rebus.Sagas;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.CosmosDb.Sagas
{
    public class TypePartitionedSagaStorage : ISagaStorage
    {
        const string SagaDataName = "sagaData";

        public static readonly string EtagKey = "rbs2-cosmos-etag";

        private static readonly Func<Type, Container, Task<Action<JObject, string>>> _partitionKeySetterFactory = (type, container) => Util.CreateSetPartitionKeyAction(type, container);

        private readonly ConcurrentDictionary<Type, Task<Action<JObject, string>>> _partitionKeySetter = new ConcurrentDictionary<Type, Task<Action<JObject, string>>>();

        private readonly IProvideSagaDataTypeInfo _typeInfoProvider;
        private readonly Func<Type, JsonSerializer> _serializerFactory;
        private readonly Func<Type, Task<Container>> _containerFactory;
        private readonly Func<ConcurrentDictionary<string, object>> _getContextBag;

        public TypePartitionedSagaStorage(Func<Type, Task<Container>> containerFactory,
            IProvideSagaDataTypeInfo typeInfoProvider, TypePartitionedSagaStorageOptions? options = null)
        {
            _containerFactory = containerFactory ?? throw new ArgumentNullException(nameof(containerFactory));
            _typeInfoProvider = typeInfoProvider ?? throw new ArgumentNullException(nameof(typeInfoProvider));
            _serializerFactory = options?.SerializerFactory ?? Util.DefaultSerializerFactory;
            _getContextBag = options?.ContextBagFactory ?? Util.GetContextBag;
        }


        public async Task Delete(ISagaData sagaData)
        {
            var pk = _typeInfoProvider.GetPartitionKey(sagaData.GetType());
            var id = sagaData.Id.ToString("D", CultureInfo.InvariantCulture);
            var container = await _containerFactory(sagaData.GetType()).ConfigureAwait(false);
            var etag = GetEtag();

            //fix: BasicLoadAndSaveAndFindOperations.BasicLoadAndSaveAndFindOperations
            sagaData.Revision++;

            var opts = new ItemRequestOptions()
            {
                IfMatchEtag = etag,
                EnableContentResponseOnWrite = false
            };
            try
            {
                await container.DeleteItemAsync<JObject>(id, new PartitionKey(pk), opts).ConfigureAwait(false);
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                //do nothing saga was already deleted
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.PreconditionFailed)
            {
                throw new ConcurrencyException(ex, $"Looks like someone has changed the saga [id: {id}, partitionKey: {pk}] instance underneath you");
            }
        }

        public async Task<ISagaData?> Find(Type sagaDataType, string propertyName, object propertyValue)
        {
            var task = propertyName == nameof(SagaData.Id)
                ? Read(sagaDataType, propertyName, propertyValue)
                : Query(sagaDataType, propertyName, propertyValue);
            var saga = await task;
            return saga;
        }

        private async Task<ISagaData?> Read(Type sagaDataType, string propertyName, object propertyValue)
        {
            var pk = _typeInfoProvider.GetPartitionKey(sagaDataType);
            var container = await _containerFactory(sagaDataType).ConfigureAwait(false);
            try
            {
                var resp = await container.ReadItemAsync<JObject>(propertyValue.ToString(), new PartitionKey(pk)).ConfigureAwait(false);
                //remember to store the etag
                SetEtag(resp.ETag);
                var sagaData = resp.Resource[SagaDataName].ToObject(sagaDataType);
                return (ISagaData)sagaData;

            }
            catch(CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return null;
            }
        }

        private async Task<ISagaData?> Query(Type sagaDataType, string propertyName, object propertyValue)
        {
            var pk = _typeInfoProvider.GetPartitionKey(sagaDataType);
            var container = await _containerFactory(sagaDataType).ConfigureAwait(false);

            var dbPropName = "$" + propertyName;
            var query = new QueryDefinition($"select * from c where c[\"{dbPropName}\"] = @propValue")
                .WithParameter("@propValue", propertyValue);
            var resp = container.GetItemQueryIterator<JObject>(query, null, new QueryRequestOptions()
            {
                MaxItemCount = 2,
                PartitionKey = new PartitionKey(pk)
            });
            if(!resp.HasMoreResults)
            {
                return null; 
            }
            var results = await resp.ReadNextAsync();
            if(results.Count == 0)
            {
                return null;
            }
            if(results.Count > 1)
            {
                throw new MultipleSagasMatchedException(
                    $"Correlation property {propertyName} with value {propertyValue} for saga data type {sagaDataType} yielded partition {pk} and db property {dbPropName} which matched multiple sagas",
                    sagaDataType,
                    propertyName,
                    propertyValue
                    );
            }

            SetEtag(results.ETag);
            var obj = results.First();
            var sagaData = obj[SagaDataName].ToObject(sagaDataType);
            return (ISagaData)sagaData;
        }

        public async Task Insert(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            if (sagaData.Id == Guid.Empty)
            {
                throw new InvalidOperationException($"Saga data {sagaData.GetType()} has an uninitialized Id property!");
            }

            if (sagaData.Revision != 0)
            {
                throw new InvalidOperationException($"Attempted to insert saga data with ID {sagaData.Id} and revision {sagaData.Revision}, but revision must be 0 on first insert!");
            }

            var pk = _typeInfoProvider.GetPartitionKey(sagaData.GetType());
            var container = await _containerFactory(sagaData.GetType()).ConfigureAwait(false);
            var doc = await Create(sagaData, container, sagaData.Id.ToString("D", CultureInfo.InvariantCulture), pk, correlationProperties);

            try
            {
                var response = await container.CreateItemAsync(doc, new PartitionKey(pk), new ItemRequestOptions()
                {
                    EnableContentResponseOnWrite = false
                });
            }
            catch(CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.Conflict)
            {
                var id = sagaData.Id.ToString("D", CultureInfo.InvariantCulture);
                throw new ConcurrencyException(ex, $"Error creating saga [id: {id}, pk: {pk}] because another saga with same id/pk exists");
            }
        }

        public async Task Update(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            var pk = _typeInfoProvider.GetPartitionKey(sagaData.GetType());
            var id = sagaData.Id.ToString("D", CultureInfo.InvariantCulture);
            var container = await _containerFactory(sagaData.GetType()).ConfigureAwait(false);
            var etag = GetEtag();
            sagaData.Revision++;
            var doc = await Create(sagaData, container, id, pk, correlationProperties).ConfigureAwait(false);
            try
            {
                var response = await container.ReplaceItemAsync(doc, id, new PartitionKey(pk), new ItemRequestOptions()
                {
                    EnableContentResponseOnWrite = false,
                    IfMatchEtag = etag
                });
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.PreconditionFailed)
            {
                throw new ConcurrencyException(ex, $"Error updating saga [id: {id}, pk: {pk}] because some else has updated the saga");
            }
        }

        void SetEtag(string etag)
            => _getContextBag()[EtagKey] = etag;

        string? GetEtag()
        {
            var ctx = _getContextBag();
            if (ctx.TryGetValue(EtagKey, out var value))
                return (string)value;
            //fix: BasicLoadAndSaveAndFindOperations.RevisionIsIncrementedOnPassedInInstanceWhenDeleting
            //should never happen in prod since we always find saga data from storage which should set the
            //etag if saga is found (Etag is only needed for update and delete)
            return null;
        }

        async ValueTask<JObject> Create(ISagaData sagaData, Container container, 
            string id, string partitionKey,
            IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            var serializer = _serializerFactory(sagaData.GetType());
            var obj = JObject.FromObject(new
            {
                Id = id,
                SagaData = sagaData
            }, serializer);
            foreach(var p in correlationProperties)
            {
                if(string.Equals(p.PropertyName, nameof(ISagaData.Id)))
                {
                    continue; //don't index id property, it won't be used to query
                }
                obj["$" + p.PropertyName] = new JValue(GetPropertyValue(sagaData, p.PropertyName));
            }
            var setter = await _partitionKeySetter.GetOrAdd(sagaData.GetType(), _partitionKeySetterFactory, container);
            setter(obj, partitionKey);
            return obj;
        }

        object GetPropertyValue(ISagaData sagaData, string property)
        {
            return sagaData.GetType()
                .GetProperty(property, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .GetValue(sagaData);
        }
    }
}
