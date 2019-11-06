using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using Storage.Net.Messaging;
using Microsoft.Azure.EventHubs;
using System;
using System.Threading;

namespace Storage.Net.Microsoft.Azure.EventHub
{
   /// <summary>
   /// Publishes messages to Azure Event Hub
   /// </summary>
   class AzureEventHubMessenger : IMessenger
   {
      private readonly EventHubClient _client;
      private readonly string _entityName;
      private readonly string _connectionString;

      //for the receiving end
      private readonly string _azureBlobStorageConnectionString;
      private readonly string _consumerGroupName;
      private readonly string _leaseContainerName;
      private readonly string _storageBlobPrefix;

      /// <summary>
      /// Creates an instance of event hub publisher by full connection string. Use this if you only need to sumbit messages.
      /// </summary>
      /// <param name="connectionString">Full connection string, including entity name</param>
      public AzureEventHubMessenger(string connectionString)
      {
         if(connectionString is null)
            throw new ArgumentNullException(nameof(connectionString));
         var csb = new EventHubsConnectionStringBuilder(connectionString);
         _connectionString = csb.ToString();
         _client = EventHubClient.CreateFromConnectionString(connectionString);
         _entityName = csb.EntityPath;
      }

      public AzureEventHubMessenger(
         string eventHubConnectionString,
         string azureBlobStorageConnectionString,
         string consumerGroupName = null,
         string leaseContainerName = null,
         string storageBlobPrefix = null) : this(eventHubConnectionString)
      {
         _azureBlobStorageConnectionString = azureBlobStorageConnectionString;
         _consumerGroupName = consumerGroupName;
         _leaseContainerName = leaseContainerName;
         _storageBlobPrefix = storageBlobPrefix;
      }

      public AzureEventHubMessenger(string namespaceName, string entityName, string keyName, string key)
      {
         var csb = new EventHubsConnectionStringBuilder(
            new Uri($"{namespaceName}.servicebus.windows.net"), entityName, keyName, key);
         _entityName = entityName;
         _client = EventHubClient.CreateFromConnectionString(csb.ToString());
         _connectionString = csb.ToString();
      }

      private static Task ThrowManagementNotSupportedException()
      {
         throw new NotSupportedException("EventHub management operations are not supported by this library as they require elevated permissions");
      }

      #region [ IMessenger ]

      public Task CreateChannelsAsync(IEnumerable<string> channelNames, CancellationToken cancellationToken = default)
      {
         return ThrowManagementNotSupportedException();
      }

      public Task<IReadOnlyCollection<string>> ListChannelsAsync(CancellationToken cancellationToken = default)
      {
         return Task.FromResult<IReadOnlyCollection<string>>(new[] { _entityName });
      }

      public Task DeleteChannelsAsync(IEnumerable<string> channelNames, CancellationToken cancellationToken = default)
      {
         if(channelNames is null)
            throw new ArgumentNullException(nameof(channelNames));

         return ThrowManagementNotSupportedException();
      }
      public Task<long> GetMessageCountAsync(string channelName, CancellationToken cancellationToken = default)
      {
         if(channelName is null)
            throw new ArgumentNullException(nameof(channelName));

         ThrowManagementNotSupportedException();

         return Task.FromResult(0L);
      }

      public async Task SendAsync(string channelName, IEnumerable<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         if(channelName is null)
            throw new ArgumentNullException(nameof(channelName));

         if(messages is null)
            throw new ArgumentNullException(nameof(messages));

         if(channelName != _entityName)
            throw new ArgumentException($"You can only send messages to '{_entityName}' channel", nameof(channelName));

         await _client.SendAsync(messages.Select(Converter.ToEventData)).ConfigureAwait(false);
      }

      public Task<IReadOnlyCollection<QueueMessage>> ReceiveAsync(
         string channelName, int count = 100, TimeSpan? visibility = null, CancellationToken cancellationToken = default)
      {
         if(channelName is null)
            throw new ArgumentNullException(nameof(channelName));

         throw new NotSupportedException();
      }

      public Task<IReadOnlyCollection<QueueMessage>> PeekAsync(string channelName, int count = 100, CancellationToken cancellationToken = default)
      {
         if(channelName is null)
            throw new ArgumentNullException(nameof(channelName));

         throw new NotSupportedException();
      }

      /// <summary>
      /// Closes the receiver
      /// </summary>
      public void Dispose()
      {
         _client.Close();
      }

      public Task DeleteAsync(string channelName, IEnumerable<QueueMessage> messages, CancellationToken cancellationToken = default) => throw new NotSupportedException();

      public async Task StartMessageProcessorAsync(string channelName, IMessageProcessor messageProcessor)
      {
         if(channelName is null)
            throw new ArgumentNullException(nameof(channelName));
         if(messageProcessor is null)
            throw new ArgumentNullException(nameof(messageProcessor));

         if(channelName != _entityName)
            throw new ArgumentException($"You can only process messages on '{_entityName}' channel", nameof(channelName));

         await new EventHubMessageProcessor(
            messageProcessor,
            _connectionString,
            _azureBlobStorageConnectionString,
            _consumerGroupName,
            _leaseContainerName,
            _storageBlobPrefix)
            .StartAsync()
            .ConfigureAwait(false);
      }

      #endregion
   }
}
