using System;
using Storage.Net.Messaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;

namespace Storage.Net.Microsoft.Azure.Storage.Messaging
{
   /// <summary>
   /// Azure Storage queue publisher
   /// </summary>
   class AzureStorageQueuePublisher : IMessagePublisher
   {
      private readonly CloudQueue _queue;

      /// <summary>
      /// Creates an instance of Azure Storage Queue by account details and the queue name
      /// </summary>
      /// <param name="accountName">Storage account name</param>
      /// <param name="storageKey">Storage key (primary or secondary)</param>
      /// <param name="queueName">Name of the queue. If queue doesn't exist it will be created</param>
      public AzureStorageQueuePublisher(string accountName, string storageKey, string queueName)
      {
         if(accountName == null) throw new ArgumentNullException(nameof(accountName));
         if(storageKey == null) throw new ArgumentNullException(nameof(storageKey));
         if(queueName == null) throw new ArgumentNullException(nameof(queueName));

         var account = new CloudStorageAccount(new StorageCredentials(accountName, storageKey), true);
         CloudQueueClient client = account.CreateCloudQueueClient();
         _queue = client.GetQueueReference(queueName);
         _queue.CreateIfNotExistsAsync().Wait();
      }

      /// <summary>
      /// For use with local development storage.
      /// Creates an instance of Azure Storage Queue by account details and the queue name
      /// </summary>
      /// <param name="queueName">Name of the queue. If queue doesn't exist it will be created</param>
      public AzureStorageQueuePublisher(string queueName)
      {
         if(queueName == null)
            throw new ArgumentNullException(nameof(queueName));

         if(CloudStorageAccount.TryParse(Constants.UseDevelopmentStorageConnectionString, out CloudStorageAccount account))
         {
            CloudQueueClient client = account.CreateCloudQueueClient();
            _queue = client.GetQueueReference(queueName);
            _queue.CreateIfNotExistsAsync().Wait();
         }
         else
         {
            throw new InvalidOperationException($"Cannot connect to local development environment when creating queue publisher.");
         }
      }

      /// <summary>
      /// Pushes new messages to storage queue. Due to the fact storage queues don't support batched pushes
      /// this method makes a call per message.
      /// </summary>
      public async Task PutMessagesAsync(IEnumerable<QueueMessage> messages, CancellationToken cancellationToken)
      {
         if (messages == null) return;
         foreach (QueueMessage message in messages)
         {
            CloudQueueMessage nativeMessage = Converter.ToCloudQueueMessage(message);
            await _queue.AddMessageAsync(nativeMessage);
            message.Id = Converter.CreateId(nativeMessage);
         }
      }

      public void Dispose()
      {
      }
   }
}
