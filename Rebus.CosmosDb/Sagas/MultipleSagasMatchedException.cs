using System;
using System.Collections.Generic;
using System.Text;

namespace Rebus.CosmosDb.Sagas
{
    public class MultipleSagasMatchedException : Exception
    {
        public MultipleSagasMatchedException(string message, Type sagaDataType, object propertyName, object propertyValue) : base(message)
        {
            SagaDataType = sagaDataType;
            PropertyName = propertyName;
            PropertyValue = propertyValue;
        }

        public Type SagaDataType { get; }
        public object PropertyName { get; }
        public object PropertyValue { get; }
    }
}
