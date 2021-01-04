using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using Rebus.Exceptions;
using Rebus.Pipeline;
using Rebus.Sagas;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.CosmosDb.Sagas
{
    /// <summary>
    /// Cosmos saga storage, stores saga data partitioned by the **SINGLE** correlation property.
    /// </summary>
    public class CosmosSagaStorage : ISagaStorage
    {
        private const string
            IdKey = "rbs2-cosmos-saga-id",
            PartitionKey = "rbs2-cosmos-saga-pk",
            EtagKey = "rbs2-cosmos-saga-etag";

        private static readonly Func<Type, Container, Task<Action<JObject, string>>> _partitionKeySetterFactory = (type, container) => Util.CreateSetPartitionKeyAction(type, container);

        private readonly ConcurrentDictionary<Type, PropertyInfo> _correlationProperties = new ConcurrentDictionary<Type, PropertyInfo>();
        private readonly ConcurrentDictionary<Type, Task<Action<JObject, string>>> _partitionKeySetter = new ConcurrentDictionary<Type, Task<Action<JObject, string>>>();

        private readonly Func<Type, Task<Container>> _containerFactory;
        private readonly Func<ConcurrentDictionary<string, object>> _getContextBag;
        private readonly Func<Type, JsonSerializer> _serializerFactory;
        private readonly Func<Type, string, object, string> _partitionKeyValueBuilder;

        public CosmosSagaStorage(Func<Type, Task<Container>> containerFactory, CosmosSagaStorageOptions? options = null)
        {
            _containerFactory = containerFactory ?? throw new ArgumentNullException(nameof(containerFactory));
            _getContextBag = options?.ContextBagFactory ?? new Func<ConcurrentDictionary<string, object>>(Util.GetContextBag);
            _serializerFactory = options?.SerializerFactory ?? new Func<Type, JsonSerializer>(Util.DefaultSerializerFactory);
            _partitionKeyValueBuilder = options?.PartitionKeyValueBuilder ?? new Func<Type, string, object, string>(DefaultPartitionKeyBuilder);
        }

        public async Task Delete(ISagaData sagaData)
        {
            var ctxBag = _getContextBag();
            var id = (string)ctxBag[IdKey];
            var pk = (string)ctxBag[PartitionKey];
            var etag = (string)ctxBag[EtagKey];

            var container = await _containerFactory(sagaData.GetType()).ConfigureAwait(false);
            var opts = new ItemRequestOptions()
            {
                IfMatchEtag = etag,
                EnableContentResponseOnWrite = false
            };
            try
            {
                await container.DeleteItemAsync<JObject>(id, new PartitionKey(pk), opts).ConfigureAwait(false);
            }
            catch(CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                //do nothing saga was already deleted
            }
            catch(CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.PreconditionFailed)
            {
                throw new ConcurrencyException(ex, $"Looks like someone has changed the saga [id: {id}, partitionKey: {pk}] instance underneath you");
            }
        }

        public async Task<ISagaData?> Find(Type sagaDataType, string propertyName, object propertyValue)
        {
            var container = await _containerFactory(sagaDataType).ConfigureAwait(false);
            var action = _partitionKeySetter.GetOrAdd(sagaDataType, _partitionKeySetterFactory, container);

            var pk = _partitionKeyValueBuilder(sagaDataType, propertyName, propertyValue);
            var id = GetSagaId(sagaDataType, pk);
            var ctxBag = _getContextBag();
            try
            {
                var response = await container.ReadItemAsync<JObject>(id, new PartitionKey(pk)).ConfigureAwait(false);
                var sagaData = response.Resource
                    .GetValue("sagadata", StringComparison.OrdinalIgnoreCase)
                    .ToObject(sagaDataType);
                ctxBag[EtagKey] = response.ETag;
                ctxBag[IdKey] = id;
                ctxBag[PartitionKey] = pk;
                return (ISagaData)sagaData;
            }
            catch(CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return null;
            }
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

            var correlationProperty = GetCorrelationProperty(sagaData.GetType(), correlationProperties);
            var prop = _correlationProperties.GetOrAdd(sagaData.GetType(), (type, name) => type.GetProperty(name)!, correlationProperty.PropertyName);
            var value = prop.GetValue(sagaData)!;

            var container = await _containerFactory(sagaData.GetType()).ConfigureAwait(false);
            var action = _partitionKeySetter.GetOrAdd(sagaData.GetType(), _partitionKeySetterFactory, container);

            var pk = _partitionKeyValueBuilder(sagaData.GetType(), prop.Name, value);
            var obj = await CreateEntity(sagaData, action, pk).ConfigureAwait(false);          
            await container.CreateItemAsync(obj, new PartitionKey(pk)).ConfigureAwait(false);
        }

        public async Task Update(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            var correlationProperty = GetCorrelationProperty(sagaData.GetType(), correlationProperties);
            var prop = _correlationProperties.GetOrAdd(sagaData.GetType(), (type, name) => type.GetProperty(name)!, correlationProperty.PropertyName);
            var value = prop.GetValue(sagaData)!;

            var pk = _partitionKeyValueBuilder(sagaData.GetType(), prop.Name, value);
            
            var container = await _containerFactory(sagaData.GetType()).ConfigureAwait(false);
            var action = _partitionKeySetter.GetOrAdd(sagaData.GetType(), _partitionKeySetterFactory, container);

            sagaData.Revision++;
            var obj = await CreateEntity(sagaData, action, pk);
            var etag = (string)_getContextBag()[EtagKey];
            try
            {
                var id = GetSagaId(sagaData.GetType(), pk);
                await container.ReplaceItemAsync(obj, id, new PartitionKey(pk), new ItemRequestOptions()
                {
                    IfMatchEtag = etag,
                    EnableContentResponseOnWrite = false
                }).ConfigureAwait(false);
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.PreconditionFailed)
            {
                throw new ConcurrencyException(ex, $"The saga entity [id: {pk}, partition: {pk}] has been modified, please retry the operation");
            }
        }

        private async Task<JObject> CreateEntity(ISagaData sagaData, Task<Action<JObject, string>> setPartitionKey, string partitionKey, bool deleted = false)
        {
            var obj = JObject.FromObject(new
            {
                id = GetSagaId(sagaData.GetType(), partitionKey),
                Type = sagaData.GetType().FullName,
                SagaData = sagaData,
                Deleted = deleted
            }, 
            _serializerFactory(sagaData.GetType())
            );

            var action = await setPartitionKey;
            action(obj, partitionKey);
            return obj; 
        }

        static ISagaCorrelationProperty GetCorrelationProperty(Type sagaDataType, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            using var enumerator = correlationProperties.GetEnumerator();
            if(!enumerator.MoveNext())
            {
                throw new ArgumentException($"You must define exactly one correlation property for saga data {sagaDataType.FullName}. The correlation property will be used as the partition key for your saga");
            }
            var prop = enumerator.Current;
            if(enumerator.MoveNext())
            {
                throw new ArgumentException($"More than one correlation property was defined for saga data {sagaDataType.FullName}");
            }
            return prop;
        }

        static string DefaultPartitionKeyBuilder(Type sagaDataType, string propertyName, object propertyValue)
        {
            if (propertyValue is string s)
                return s;
            if (propertyValue is Guid g)
                return g.ToString("D", System.Globalization.CultureInfo.InvariantCulture);
            return propertyValue.ToString()!;
        }

        //improve on this
        static string GetSagaId(Type sagaDataType, string partitionKey)
            => $"{sagaDataType.Name.ToLowerInvariant()}-{partitionKey}";
    }
}
