using Microsoft.Azure.Cosmos;
using NUnit.Framework;
using Rebus.CosmosDb.Sagas;
using Rebus.Sagas;
using Rebus.Tests.Contracts.Sagas;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.CosmosDb.Tests.Sagas.Correlated
{
    public class CosmosDbSagaStorageFactory : ISagaStorageFactory
    {
        private const string CosmosConnectionString = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        private const string SagaDatabase = "sagas";
        private const string SagaContainer = "sagadata";

        readonly CosmosClient _client;
        readonly Container _container;

        static readonly AsyncLocal<ConcurrentDictionary<string, object>> _items
            = new AsyncLocal<ConcurrentDictionary<string, object>>();


        public CosmosDbSagaStorageFactory()
        {
            _client = new CosmosClient(CosmosConnectionString);
            _container = GetContainerTask().GetAwaiter().GetResult();
            CleanUp();
        }

        async Task<Container> GetContainerTask()
        {

            var db = await _client.CreateDatabaseIfNotExistsAsync(SagaDatabase);
            try
            {
                var container = db.Database.GetContainer(SagaContainer);
                await container.DeleteContainerAsync();
            }
            catch(CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                //do nothing
            }
            var containerResponse = await db.Database.CreateContainerIfNotExistsAsync(
                new ContainerProperties(SagaContainer, "/partitionKey")
                );
            return containerResponse.Container;
        }

        public ISagaStorage GetSagaStorage()
        {
            _items.Value = new ConcurrentDictionary<string, object>();
            return new CosmosSagaStorage(type => Task.FromResult(_container), new CosmosSagaStorageOptions()
            {
                ContextBagFactory = () => _items.Value
            });
        }

        public void CleanUp()
        {
        }
    }
}
