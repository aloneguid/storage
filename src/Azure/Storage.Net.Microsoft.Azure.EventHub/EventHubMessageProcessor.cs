using System;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Storage.Net.Messaging;

namespace Storage.Net.Microsoft.Azure.EventHub
{
   class EventHubMessageProcessor : IEventProcessorFactory
   {
      private const string DefaultConsumerGroupName = "$Default";
      private const string DefaultLeaseContainerName = "eventhubs";
      private readonly EventProcessorHost _host;
      private readonly IMessageProcessor _messageProcessor;

      public EventHubMessageProcessor(
         IMessageProcessor messageProcessor,
         string eventHubConnectionString,
         string azureBlobStorageConnectionString,
         string consumerGroupName = null,
         string leaseContainerName = null,
         string storageBlobPrefix = null)
      {
         if(eventHubConnectionString is null)
            throw new ArgumentNullException(nameof(eventHubConnectionString));

         var csb = new EventHubsConnectionStringBuilder(eventHubConnectionString);

         string entityPath = csb.EntityPath;
         csb.EntityPath = null;
         string namespaceConnectionString = csb.ToString();

         _host = new EventProcessorHost(
            Guid.NewGuid().ToString(),
            entityPath,
            consumerGroupName ?? DefaultConsumerGroupName,
            namespaceConnectionString,
            azureBlobStorageConnectionString,
            leaseContainerName ?? DefaultLeaseContainerName,
            storageBlobPrefix);

         _messageProcessor = messageProcessor;
      }

      public IEventProcessor CreateEventProcessor(PartitionContext context)
      {
         return new MessageProcessorEventProcessor(_messageProcessor, context);
      }

      public async Task StartAsync()
      {
         await _host.RegisterEventProcessorFactoryAsync(this).ConfigureAwait(false);
      }
   }
}
