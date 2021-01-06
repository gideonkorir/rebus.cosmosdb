using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;

namespace Rebus.CosmosDb.Sagas
{

    public class CosmosSagaStorageOptions
    {
        /// <summary>
        /// Serializer factory, given saga data type return a JsonSerializer
        /// instance that will be used to serialize the saga data. The default 
        /// serializer uses <see cref="Newtonsoft.Json.Serialization.CamelCasePropertyNamesContractResolver"/>
        /// </summary>
        public Func<Type, JsonSerializer>? SerializerFactory { get; set; }
        /// <summary>
        /// Function that takes the saga data type, correlation property name and value
        /// and returns the partition key value. By default it returns value.ToString()!
        /// </summary>
        public Func<Type, string, object, string>? PartitionKeyValueBuilder { get; set; }
    }
}
