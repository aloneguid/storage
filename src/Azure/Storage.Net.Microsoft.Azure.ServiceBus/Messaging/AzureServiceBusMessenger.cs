using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.Management;
using Storage.Net.Messaging;

namespace Storage.Net.Microsoft.Azure.ServiceBus.Messaging
{
   class AzureServiceBusMessenger : IAzureServiceBusMessenger
   {
      private const string TopicPrefix = "t/";
      private const string QueuePrefix = "q/";

      private readonly ManagementClient _mgmt;

      private readonly string _connectionString;
      private readonly string _entityPath;

      private readonly ConcurrentDictionary<string, QueueClient> _channelNameToQueueClient =
         new ConcurrentDictionary<string, QueueClient>();

      private readonly ConcurrentDictionary<string, TopicClient> _channelNameToTopicClient =
         new ConcurrentDictionary<string, TopicClient>();

      private readonly ConcurrentDictionary<string, MessageReceiver> _channelNameToMessageReceiver =
         new ConcurrentDictionary<string, MessageReceiver>();

      public AzureServiceBusMessenger(string connectionString)
      {
         _connectionString = connectionString;
         _entityPath = GetEntityPath(connectionString);
         if(_entityPath == null)
         {
            _mgmt = new ManagementClient(connectionString);
         }
      }

      private static string GetEntityPath(string cs)
      {
         return null;
      }

      ISenderClient CreateOrGetSenderClient(string channelName)
      {
         Decompose(channelName, out string entityPath, out bool isQueue);

         if(isQueue)
            return _channelNameToQueueClient.GetOrAdd(channelName, cn => new QueueClient(_connectionString, entityPath, ReceiveMode.PeekLock));

         return _channelNameToTopicClient.GetOrAdd(channelName, cn => new TopicClient(_connectionString, entityPath));
      }

      IReceiverClient CreateOrGetReceiverClient(string channelName)
      {
         Decompose(channelName, out string entityPath, out bool isQueue);

         if(isQueue)
            return _channelNameToQueueClient.GetOrAdd(channelName, cn => new QueueClient(_connectionString, entityPath, ReceiveMode.PeekLock));

         throw new NotImplementedException();
      }

      MessageReceiver CreateMessageReceiver(string channelName)
      {
         Decompose(channelName, out string entityPath, out bool isQueue);

         if(isQueue)
            return _channelNameToMessageReceiver.GetOrAdd(channelName, cn => new MessageReceiver(_connectionString, entityPath, ReceiveMode.PeekLock));

         throw new NotSupportedException();
      }


      private static void Decompose(string channelName, out string entityPath, out bool isQueue)
      {
         if(channelName.StartsWith(QueuePrefix))
         {
            entityPath = channelName.Substring(2);
            isQueue = true;
            return;
         }

         if(channelName.StartsWith(TopicPrefix))
         {
            entityPath = channelName.Substring(2);
            isQueue = false;
            return;
         }

         throw new ArgumentException(
            $"Channel '{channelName}' is not a valid channel name. It should start with '{QueuePrefix}' for queues or '{TopicPrefix}' for topics",
            nameof(channelName));
      }

      private static void DecomposeSubscription(string channelName, out string topicPath, out string subscriptionName)
      {
         Decompose(channelName, out string allPath, out bool isQueue);

         int idx = allPath.IndexOf('/');
         if(idx == -1)
         {
            throw new ArgumentException($"channel '{channelName}' does not contain topic and subscription name, it should look like '{TopicPrefix}topic_name/subscription_name'", nameof(channelName));
         }
         topicPath = allPath.Substring(0, idx);
         subscriptionName = allPath.Substring(idx + 1);
      }

      #region [ IMessenger ]

      public async Task CreateChannelsAsync(IEnumerable<string> channelNames, CancellationToken cancellationToken = default)
      {
         foreach(string channelName in channelNames)
         {
            Decompose(channelName, out string entityPath, out bool isQueue);

            if(isQueue)
            {
               await _mgmt.CreateQueueAsync(entityPath, cancellationToken).ConfigureAwait(false);
            }
            else
            {
               await _mgmt.CreateTopicAsync(entityPath, cancellationToken).ConfigureAwait(false);
            }
         }
      }


      public async Task SendAsync(string channelName, IEnumerable<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         ISenderClient client = CreateOrGetSenderClient(channelName);

         IList<Message> sbmsg = messages.Select(Converter.ToMessage).ToList();
         await client.SendAsync(sbmsg).ConfigureAwait(false);
      }

      public async Task DeleteChannelsAsync(IEnumerable<string> channelNames, CancellationToken cancellationToken = default)
      {
         if(channelNames is null)
            throw new ArgumentNullException(nameof(channelNames));

         foreach(string cn in channelNames)
         {
            Decompose(cn, out string entityPath, out bool isQueue);

            if(isQueue)
               await _mgmt.DeleteQueueAsync(entityPath, cancellationToken).ConfigureAwait(false);
            else
               await _mgmt.DeleteQueueAsync(entityPath, cancellationToken).ConfigureAwait(false);
         }
      }

      public async Task<long> GetMessageCountAsync(string channelName, CancellationToken cancellationToken = default)
      {
         if(channelName is null)
            throw new ArgumentNullException(nameof(channelName));

         Decompose(channelName, out string entityPath, out bool isQueue);

         if(isQueue)
         {

            try
            {
               QueueRuntimeInfo qinfo = await _mgmt.GetQueueRuntimeInfoAsync(entityPath, cancellationToken).ConfigureAwait(false);
               return qinfo.MessageCount;
            }
            catch(MessagingEntityNotFoundException)
            {
               return 0;
            }
         }

         DecomposeSubscription(channelName, out string topicPath, out string subscriptionName);
         throw new NotSupportedException();

         /*try
         {
            SubscriptionRuntimeInfo info = await _mgmt.GetSubscriptionRuntimeInfoAsync(topicPath, subscriptionName, cancellationToken).ConfigureAwait(false);
            return info.MessageCount;
         }
         catch(MessagingEntityNotFoundException)
         {
            return 0;
         }*/
      }

      public async Task<IReadOnlyCollection<string>> ListChannelsAsync(CancellationToken cancellationToken = default)
      {
         if(_entityPath != null)
            return new List<string> { _entityPath };

         var channels = new List<string>();

         IList<QueueDescription> queues = await _mgmt.GetQueuesAsync().ConfigureAwait(false);
         IList<TopicDescription> topics = await _mgmt.GetTopicsAsync().ConfigureAwait(false);

         channels.AddRange(queues.Select(d => $"{QueuePrefix}{d.Path}"));
         channels.AddRange(topics.Select(d => $"{TopicPrefix}{d.Path}"));

         return channels;
      }

      public async Task<IReadOnlyCollection<QueueMessage>> PeekAsync(string channelName, int count = 100, CancellationToken cancellationToken = default)
      {
         if(channelName is null)
            throw new ArgumentNullException(nameof(channelName));

         MessageReceiver receiver = CreateMessageReceiver(channelName);
         IList<Message> messages = await receiver.PeekAsync(count).ConfigureAwait(false);

         return messages.Select(Converter.ToQueueMessage).ToList();
      }

      public async Task<IReadOnlyCollection<QueueMessage>> ReceiveAsync(
         string channelName, int count = 100, TimeSpan? visibility = null, CancellationToken cancellationToken = default)
      {
         if(channelName is null)
            throw new ArgumentNullException(nameof(channelName));

         MessageReceiver receiver = CreateMessageReceiver(channelName);
         IList<Message> messages = await receiver.ReceiveAsync(count).ConfigureAwait(false);

         return messages.Select(Converter.ToQueueMessage).ToList();
      }

      public void Dispose()
      {

      }

      #endregion

      #region [ IAzureServiceBusMessenger ]

      public async Task CreateQueueAsync(string name)
      {
         await _mgmt.CreateQueueAsync(name).ConfigureAwait(false);
      }

      public Task DeleteAsync(string channelName, IEnumerable<QueueMessage> messages, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task StartMessageProcessorAsync(string channelName, IMessageProcessor messageProcessor) => throw new NotImplementedException();


      #endregion
   }
}
