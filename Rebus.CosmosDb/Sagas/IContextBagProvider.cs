using Rebus.Pipeline;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace Rebus.CosmosDb.Sagas
{
    /// <summary>
    /// Gets the context bag associated with the current message processing
    /// </summary>
    public interface IContextBagProvider
    {
        ConcurrentDictionary<string, object> ContextBag { get; }
    }


    /// <summary>
    /// Provides a context bag from a given IMessageContext
    /// </summary>
    public class MessageContextBagProvider : IContextBagProvider
    {
        public ConcurrentDictionary<string, object> ContextBag
        {
            get
            {
                var ctx = MessageContext.Current;
                if(ctx == null)
                {
                    throw new InvalidOperationException("Unable to retrieve the current message context, this provider can only be used during normal message processing");
                }
                return ctx.TransactionContext.Items;
            }
        }
    }
}
