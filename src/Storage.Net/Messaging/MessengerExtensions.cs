using System;
using System.Threading;
using System.Threading.Tasks;

namespace Storage.Net.Messaging
{
   /// <summary>
   /// Extensions for <see cref="IMessenger"/>
   /// </summary>
   public static class MessengerExtensions
   {
      /// <summary>
      /// Create a channel
      /// </summary>
      /// <param name="messenger"></param>
      /// <param name="channelName"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public static Task CreateChannelAsync(this IMessenger messenger, string channelName, CancellationToken cancellationToken = default)
      {
         return messenger.CreateChannelsAsync(new[] { channelName }, cancellationToken);
      }

      /// <summary>
      /// Puts a new message to the back of the queue.
      /// </summary>
      public static Task SendAsync(this IMessenger messenger, string channelName, QueueMessage message, CancellationToken cancellationToken = default)
      {
         if(channelName is null)
            throw new ArgumentNullException(nameof(channelName));

         if(message == null)
            throw new ArgumentNullException(nameof(message));

         return messenger.SendAsync(channelName, new[] { message }, cancellationToken);
      }

      /// <summary>
      /// Deletes a single channel
      /// </summary>
      /// <param name="messenger"></param>
      /// <param name="channelName"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public static Task DeleteChannelAsync(this IMessenger messenger, string channelName, CancellationToken cancellationToken = default)
      {
         return messenger.DeleteChannelsAsync(new[] { channelName }, cancellationToken);
      }
   }
}
