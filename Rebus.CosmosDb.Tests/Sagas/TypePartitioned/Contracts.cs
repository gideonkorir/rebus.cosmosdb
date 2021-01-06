using NUnit.Framework;
using Rebus.CosmosDb.Sagas;
using Rebus.Sagas;
using Rebus.Tests.Contracts.Sagas;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Rebus.CosmosDb.Tests.Sagas.TypePartitioned
{
    [TestFixture, Category(Categories.CosmosDb)]
    public class TypePartitionedCosmosDbSagaStorageBasicLoadAndSaveAndFindOperations : BasicLoadAndSaveAndFindOperations<TypePartitionedCosmosDbSagaStorageFactory> { }

    [TestFixture, Category(Categories.CosmosDb)]
    public class TypePartitionedCosmosDbSagaStorageSagaIntegrationTests : SagaIntegrationTests<TypePartitionedCosmosDbSagaStorageFactory> { }

    [TestFixture]
    public class TypePartitionedTestSagaCorrelationCosmosDb : TestSagaCorrelation<TypePartitionedCosmosDbSagaStorageFactory> { }

    [TestFixture, Category(Categories.CosmosDb)]
    public class TypePartitionedCosmosDbSagaStorageConcurrencyHandling : ConcurrencyHandling<TypePartitionedCosmosDbSagaStorageFactory> 
    {
        readonly IEnumerable<ISagaCorrelationProperty> _noCorrelationProperties = Enumerable.Empty<ISagaCorrelationProperty>();
        [Test]
        public async Task EtagIsSetWhenFindReturnsSagaData()
        {
            var data = new TestSagaData()
            {
                Id = Guid.NewGuid(),
                Revision = 0
            };
            var factory = new TypePartitionedCosmosDbSagaStorageFactory();
            var storage = factory.GetSagaStorage();
            await storage.Insert(data, _noCorrelationProperties);
            //insert
            Assert.False(TypePartitionedCosmosDbSagaStorageFactory.Items.Value.TryGetValue(TypePartitionedSagaStorage.EtagKey, out _));

            //find
            _ = await storage.Find(typeof(TestSagaData), nameof(ISagaData.Id), data.Id);
            Assert.True(TypePartitionedCosmosDbSagaStorageFactory.Items.Value.TryGetValue(TypePartitionedSagaStorage.EtagKey, out _));
        }

        class TestSagaData : ISagaData
        {
            public Guid Id { get; set; }
            public int Revision { get; set; }
        }
    }
}
