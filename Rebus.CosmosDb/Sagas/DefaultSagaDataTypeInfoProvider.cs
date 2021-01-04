using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using System;
using System.Reflection;
using System.Text;

namespace Rebus.CosmosDb.Sagas
{
    public class DefaultSagaDataTypeInfoProvider : IProvideSagaDataTypeInfo
    {
        private readonly CosmosPropertyNamingPolicy _namingPolicy;

        public DefaultSagaDataTypeInfoProvider(CosmosPropertyNamingPolicy namingPolicy)
        {
            _namingPolicy = namingPolicy;
        }

        public string GetPartitionKey(Type sagaDataType)
            => sagaDataType.FullName.ToLowerInvariant();

        public string GetPropertyStorageName(Type sagaDataType, string propertyName)
        {
            //should probably cache this
            var paths = propertyName.Split();
            Type start = sagaDataType;
            StringBuilder builder = new StringBuilder(propertyName.Length);
            for (int i = 0; i < paths.Length - 1; i++)
            {
                start = AppendSingleProperty(builder, start, paths[i], _namingPolicy);
                builder.Append('.');
            }

            _ = AppendSingleProperty(builder, start, paths[^1], _namingPolicy);

            return builder.ToString();

            static Type AppendSingleProperty(StringBuilder builder, Type type, string propertyName, CosmosPropertyNamingPolicy namingPolicy)
            {
                var prop = type.GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                var attr = prop.GetCustomAttribute<JsonPropertyAttribute>();
                if (attr != null && !string.IsNullOrWhiteSpace(attr.PropertyName))
                {
                    builder.Append(attr.PropertyName);
                }
                else if (namingPolicy == CosmosPropertyNamingPolicy.CamelCase)
                {
                    builder.Append(char.ToLowerInvariant(propertyName[0]))
                        .Append(propertyName.AsSpan(1));
                }
                else
                {
                    builder.Append(propertyName);
                }
                return prop.PropertyType;
            }
        }
    }
}
