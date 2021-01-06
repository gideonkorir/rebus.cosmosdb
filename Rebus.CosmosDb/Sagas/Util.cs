using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using Rebus.Pipeline;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.CosmosDb.Sagas
{
    internal static class Util
    {
        public static JsonSerializer DefaultSerializerFactory(Type sagaDataType)
        {
            return new JsonSerializer()
            {
                ContractResolver = new CamelCasePropertyNamesContractResolver()
            };
        }

        public static async Task<Action<JObject, string>> CreateSetPartitionKeyAction(Type sagaDataType, Container container, string ignoreIfStartsWith = "sagadata")
        {
            var containerProperties = await container.ReadContainerAsync().ConfigureAwait(false);
            var partitionKeyPath = containerProperties.Resource.PartitionKeyPath;
            var segments = partitionKeyPath.Split("/", StringSplitOptions.RemoveEmptyEntries);
            if (string.Equals(segments[0], ignoreIfStartsWith, StringComparison.OrdinalIgnoreCase))
            {
                return (obj, value) => { };
            }
            //return the action
            return (obj, value) =>
            {
                if (segments.Length == 1)
                {
                    obj.Add(segments[0], value);
                }
                else
                {
                    JObject current = new JObject();
                    obj.Add(segments[0], current);

                    for (int i = 1; i < segments.Length - 1; i++)
                    {
                        var temp = new JObject();
                        current.Add(segments[i], temp);
                        current = temp;
                    }

                    current.Add(segments[^1], value);
                }
            };
        }
    }
}
