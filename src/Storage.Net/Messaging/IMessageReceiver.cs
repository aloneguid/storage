using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Storage.Net.Messaging
{
   /// <summary>
   /// Responsible for receiving messages
   /// </summary>
   public interface IMessageReceiver : IDisposable
   {
      /// <summary>
      /// Fetches count of messages currently in the queue.
      /// </summary>
      /// <returns></returns>
      /// <exception cref="NotSupportedException">Thrown when this implementation doesn't support counting.</exception>
      Task<int> GetMessageCountAsync();

      /// <summary>
      /// Confirmation call that the message was acknowledged and processed by the receiver.
      /// Client must call this when message processing has succeeded, otherwise the message will reappear,
      /// however this depends on implementation details when and how.
      /// </summary>
      /// <param name="messages"></param>
      /// <param name="cancellationToken"></param>
      Task ConfirmMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default);

      /// <summary>
      /// Moves the message to a dead letter queue
      /// </summary>
      Task DeadLetterAsync(QueueMessage message, string reason, string errorDescription, CancellationToken cancellationToken = default);

      /// <summary>
      /// Peeks messages from the queue, without changing their visibility or removing from the queue.
      /// </summary>
      /// <param name="maxMessages">Maximum number of messages to peek.</param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      /// <exception cref="NotSupportedException">Thrown when peeking is not supported by the current provider</exception>
      Task<IReadOnlyCollection<QueueMessage>> PeekMessagesAsync(int maxMessages, CancellationToken cancellationToken = default);

      /// <summary>
      /// Starts automatic message pumping trying to use native features as much as possible. Message pump stops when you dispose the instance.
      /// Disposing the instance will also stop message pump for you.
      /// </summary>
      Task StartMessagePumpAsync(Func<IReadOnlyCollection<QueueMessage>, CancellationToken, Task> onMessageAsync, int maxBatchSize = 1, CancellationToken cancellationToken = default);

      /// <summary>
      /// Notifies the backend that processing is still happening and message should be marked alive.
      /// </summary>
      /// <param name="message"></param>
      /// <param name="timeToLive">Optional time-to-live</param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      Task KeepAliveAsync(QueueMessage message, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default);

      /// <summary>
      /// Starts a new transaction
      /// </summary>
      /// <returns></returns>
      Task<ITransaction> OpenTransactionAsync();
   }
}
