using Rebus.CosmosDb.Sagas;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Rebus.CosmosDb.Tests.Sagas
{
    class TestContextBagProvider : IContextBagProvider
    {
        internal static AsyncLocal<ConcurrentDictionary<string, object>> Items { get; }  =  new AsyncLocal<ConcurrentDictionary<string, object>>();

        public ConcurrentDictionary<string, object> ContextBag => Items.Value;

        public TestContextBagProvider()
        {
            Items.Value = new ConcurrentDictionary<string, object>();
        }
    }
}
