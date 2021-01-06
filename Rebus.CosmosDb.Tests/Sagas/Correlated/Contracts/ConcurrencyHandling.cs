using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Exceptions;
using Rebus.Sagas;

namespace Rebus.Tests.Contracts.Sagas.SingleCorrelated
{
    /// <summary>
    /// Test fixture base class for verifying compliance with the <see cref="ISagaStorage"/> contract
    /// </summary>
    public abstract class ConcurrencyHandling<TFactory> : FixtureBase where TFactory : ISagaStorageFactory, new()
    {
        readonly IEnumerable<ISagaCorrelationProperty> _noCorrelationProperties = Enumerable.Empty<ISagaCorrelationProperty>();

        ISagaStorage _sagaStorage;
        TFactory _factory;

        protected override void SetUp()
        {
            _factory = new TFactory();
            _sagaStorage = _factory.GetSagaStorage();
        }

        protected override void TearDown()
        {
            CleanUpDisposables();

            _factory.CleanUp();
        }

        [Test]
        public async Task ThrowsWhenRevisionDoesNotMatchExpected()
        {
            var id = Guid.NewGuid();

            var correlationProperties = new ISagaCorrelationProperty[]
            {
                new TestCorrelationProperty(nameof(ISagaData.Id), typeof(SomeSagaData))
            };

            await _sagaStorage.Insert(new SomeSagaData { Id = id }, correlationProperties);

            var loadedData1 = await _sagaStorage.Find(typeof(SomeSagaData), "Id", id);

            var loadedData2 = await _sagaStorage.Find(typeof(SomeSagaData), "Id", id);

            await _sagaStorage.Update(loadedData1, correlationProperties);

            var aggregateException = Assert.Throws<AggregateException>(() => _sagaStorage.Update(loadedData2, correlationProperties).Wait());

            var baseException = aggregateException.GetBaseException();

            Assert.That(baseException, Is.TypeOf<ConcurrencyException>());
        }

        class SomeSagaData : ISagaData
        {
            public Guid Id { get; set; }
            public int Revision { get; set; }
        }
    }
}