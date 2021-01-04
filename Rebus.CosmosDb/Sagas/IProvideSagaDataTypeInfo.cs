using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;

namespace Rebus.CosmosDb.Sagas
{
    /// <summary>
    /// Retrieves the saga partition key value based on
    /// the saga data type
    /// </summary>
    public interface IProvideSagaDataTypeInfo
    {
        string GetPartitionKey(Type sagaDataType);

        string GetPropertyStorageName(Type sagaDataType, string propertyName);
    }
}
