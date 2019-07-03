using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Storage.Net.Messaging
{
   /// <summary>
   /// Responsible for publishing messages
   /// </summary>
   public interface IMessagePublisher : IDisposable
   {
      /// <summary>
      /// Puts a new batch of messages to the back of the queue as quick and efficient as possible for
      /// a given queue implementation.
      /// </summary>
      Task PutMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default);
   }
}
