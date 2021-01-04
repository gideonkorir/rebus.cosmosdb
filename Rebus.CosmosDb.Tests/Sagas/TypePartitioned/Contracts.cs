using NUnit.Framework;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.CosmosDb.Tests.Sagas.TypePartitioned
{
    [TestFixture, Category(Categories.CosmosDb)]
    public class TypePartitionedCosmosDbSagaStorageBasicLoadAndSaveAndFindOperations : BasicLoadAndSaveAndFindOperations<TypePartitionedCosmosDbSagaStorageFactory> { }

    [TestFixture, Category(Categories.CosmosDb)]
    public class TypePartitionedCosmosDbSagaStorageConcurrencyHandling : ConcurrencyHandling<TypePartitionedCosmosDbSagaStorageFactory> { }

    [TestFixture, Category(Categories.CosmosDb)]
    public class TypePartitionedCosmosDbSagaStorageSagaIntegrationTests : SagaIntegrationTests<TypePartitionedCosmosDbSagaStorageFactory> { }

    [TestFixture]
    public class TypePartitionedTestSagaCorrelationCosmosDb : TestSagaCorrelation<TypePartitionedCosmosDbSagaStorageFactory> { }
}
