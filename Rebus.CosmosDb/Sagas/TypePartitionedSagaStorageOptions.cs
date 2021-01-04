using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;

namespace Rebus.CosmosDb.Sagas
{
    public class TypePartitionedSagaStorageOptions
    {
        public Func<Type, JsonSerializer>? SerializerFactory { get; set; }
        public Func<ConcurrentDictionary<string, object>>? ContextBagFactory { get; set; }
    }
}
