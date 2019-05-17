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
   public class AzureEventHubPublisher : IMessagePublisher
   {
      private readonly EventHubClient _client;

      /// <summary>
      /// Creates an instance of event hub publisher by full connection string
      /// </summary>
      /// <param name="connectionString">Full connection string</param>
      public AzureEventHubPublisher(string connectionString)
      {
         ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));

         _client = EventHubClient.CreateFromConnectionString(connectionString);
      }

      /// <summary>
      /// Full connection string used to connect to EventHub
      /// </summary>
      public string ConnectionString { get; private set; }

      /// <summary>
      /// Create with connection string and entity path
      /// </summary>
      /// <param name="connectionString">Connection string</param>
      /// <param name="path">Entity path</param>
      /// <returns></returns>
      public static AzureEventHubPublisher Create(string connectionString, string path)
      {
         if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
         if (path == null) throw new ArgumentNullException(nameof(path));

         var csb = new EventHubsConnectionStringBuilder(connectionString)
         {
            EntityPath = path
         };

         return new AzureEventHubPublisher(csb.ToString());
      }

      /// <summary>
      /// The most detailed method with full fragmentation
      /// </summary>
      /// <param name="endpointAddress">Endpoint address</param>
      /// <param name="entityPath">Entity path</param>
      /// <param name="sharedAccessKeyName">Shared access key name</param>
      /// <param name="sharedAccessKey">Shared access key value</param>
      /// <returns></returns>
      public static AzureEventHubPublisher Create(Uri endpointAddress, string entityPath, string sharedAccessKeyName, string sharedAccessKey)
      {
         if (endpointAddress == null) throw new ArgumentNullException(nameof(endpointAddress));
         if (entityPath == null) throw new ArgumentNullException(nameof(entityPath));
         if (sharedAccessKeyName == null) throw new ArgumentNullException(nameof(sharedAccessKeyName));
         if (sharedAccessKey == null) throw new ArgumentNullException(nameof(sharedAccessKey));

         var csb = new EventHubsConnectionStringBuilder(endpointAddress, entityPath, sharedAccessKeyName, sharedAccessKey);

         return new AzureEventHubPublisher(csb.ToString());
      }

      /// <summary>
      /// See interface
      /// </summary>
      public async Task PutMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         if (messages == null) return;

         await _client.SendAsync(messages.Select(Converter.ToEventData));
      }

      /// <summary>
      /// Closes the receiver
      /// </summary>
      public void Dispose()
      {
         _client.CloseAsync().Wait();
      }
   }
}
