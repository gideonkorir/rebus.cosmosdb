﻿using NUnit.Framework;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.CosmosDb.Tests.Sagas
{
    [TestFixture, Category(Categories.CosmosDb)]
    public class CosmosDbSagaStorageBasicLoadAndSaveAndFindOperations : BasicLoadAndSaveAndFindOperations<CosmosDbSagaStorageFactory> { }

    [TestFixture, Category(Categories.CosmosDb)]
    public class CosmosDbSagaStorageConcurrencyHandling : ConcurrencyHandling<CosmosDbSagaStorageFactory> { }

    [TestFixture, Category(Categories.CosmosDb)]
    public class CosmosDbSagaStorageSagaIntegrationTests : SagaIntegrationTests<CosmosDbSagaStorageFactory> { }

    [TestFixture]
    public class TestSagaCorrelationCosmosDb : TestSagaCorrelation<CosmosDbSagaStorageFactory> { }
}
