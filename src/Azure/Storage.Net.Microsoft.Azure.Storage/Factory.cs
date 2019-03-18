using System;
using Storage.Net.Blob;
using Storage.Net.Messaging;
using Storage.Net.Microsoft.Azure.Storage.Blob;
using Storage.Net.Microsoft.Azure.Storage.Messaging;
using Storage.Net.Microsoft.Azure.Storage.KeyValue;
using Storage.Net.KeyValue;
using System.Net;
using Storage.Net.Microsoft.Azure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Storage.Net
{
   /// <summary>
   /// Factory class that implement factory methods for Microsoft Azure implememtations
   /// </summary>
   public static class Factory
   {

      /// <summary>
      /// Register Azure module.
      /// </summary>
      public static IModulesFactory UseAzureStorage(this IModulesFactory factory)
      {
         return factory.Use(new AzureStorageModule());
      }

      /// <summary>
      /// Creates an instance of Azure Table Storage using account name and key.
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="accountName">Account name</param>
      /// <param name="storageKey">Account key</param>
      /// <returns></returns>
      public static IKeyValueStorage AzureTableStorage(this IKeyValueStorageFactory factory,
         string accountName,
         string storageKey)
      {
         return new AzureTableStorageKeyValueStorage(accountName, storageKey);
      }

      /// <summary>
      /// Creates an instance of Azure Table Storage using account name and key.
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="credential">Credential structure cotnaining account name in username and account key in password.</param>
      /// <returns></returns>
      public static IKeyValueStorage AzureTableStorage(this IKeyValueStorageFactory factory,
         NetworkCredential credential)
      {
         return new AzureTableStorageKeyValueStorage(credential.UserName, credential.Password);
      }

      /// <summary>
      /// Creates a blob storage implementation based on Microsoft Azure Blob Storage using account name and key.
      /// </summary>
      /// <param name="factory">Reference to factory</param>
      /// <param name="accountName">Storage Account name</param>
      /// <param name="key">Storage Account key</param>
      /// <returns>Generic blob storage interface</returns>
      public static IBlobStorage AzureBlobStorage(this IBlobStorageFactory factory,
         string accountName,
         string key)
      {
         return AzureUniversalBlobStorageProvider.CreateFromAccountNameAndKey(accountName, key);
      }

      /// <summary>
      /// Creates a blob storage implementation based on Microsoft Azure Blob Storage using development storage.
      /// </summary>
      /// <param name="factory">Reference to factory</param>
      /// <returns>Generic blob storage interface</returns>
      public static IBlobStorage AzureBlobDevelopmentStorage(this IBlobStorageFactory factory)
      {
         return AzureUniversalBlobStorageProvider.CreateForLocalEmulator();
      }

      /// <summary>
      /// Creates an instance of Microsoft Azure Blob Storage that wraps around native <see cref="CloudBlobClient"/>.
      /// Avoid using if possible as it's a subject to change in future.
      /// </summary>
      public static IBlobStorage AzureBlobStorage(this IBlobStorageFactory factory, CloudBlobClient cloudBlobClient)
      {
         if(cloudBlobClient == null)
            throw new ArgumentNullException(nameof(cloudBlobClient));

         return new AzureUniversalBlobStorageProvider(cloudBlobClient);
      }

      /// <summary>
      /// Creates an instance of a publisher to Azure Storage Queues
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="accountName">Account name</param>
      /// <param name="storageKey">Storage key</param>
      /// <param name="queueName">Queue name</param>
      /// <returns>Generic message publisher interface</returns>
      public static IMessagePublisher AzureStorageQueuePublisher(this IMessagingFactory factory,
         string accountName,
         string storageKey,
         string queueName)
      {
         return new AzureStorageQueuePublisher(accountName, storageKey, queueName);
      }

      /// <summary>
      /// Creates an instance of a receiver from Azure Storage Queues
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="accountName">Account name</param>
      /// <param name="storageKey">Storage key</param>
      /// <param name="queueName">Queue name</param>
      /// <param name="messageVisibilityTimeout">Message visibility timeout</param>
      /// <returns>Generic message receiver interface</returns>
      public static IMessageReceiver AzureStorageQueueReceiver(this IMessagingFactory factory,
         string accountName,
         string storageKey,
         string queueName,
         TimeSpan messageVisibilityTimeout)
      {
         return new AzureStorageQueueReceiver(accountName, storageKey, queueName, messageVisibilityTimeout);
      }

      /// <summary>
      /// Creates an instance of a receiver from Azure Storage Queues
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="accountName">Account name</param>
      /// <param name="storageKey">Storage key</param>
      /// <param name="queueName">Queue name</param>
      /// <param name="messageVisibilityTimeout">Message visibility timeout</param>
      /// <param name="messagePollingInterval">Storage Queues do not support listening therefore internally we poll for new messages. This parameters
      /// indicates how often this happens</param>
      /// <returns>Generic message receiver interface</returns>
      public static IMessageReceiver AzureStorageQueueReceiver(this IMessagingFactory factory,
         string accountName,
         string storageKey,
         string queueName,
         TimeSpan messageVisibilityTimeout,
         TimeSpan messagePollingInterval)
      {
         return new AzureStorageQueueReceiver(accountName, storageKey, queueName, messageVisibilityTimeout, messagePollingInterval);
      }

      /// <summary>
      /// Creates an instance of Azure Table Storage using development storage.
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <returns></returns>
      public static IKeyValueStorage AzureTableDevelopmentStorage(this IKeyValueStorageFactory factory)
      {
         return new AzureTableStorageKeyValueStorage();
      }

      /// <summary>
      /// Creates an instance of a publisher to Azure Storage Queues using development storage.
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="queueName">Queue name</param>
      /// <returns>Generic message publisher interface</returns>
      public static IMessagePublisher AzureDevelopmentStorageQueuePublisher(this IMessagingFactory factory,
         string queueName)
      {
         return new AzureStorageQueuePublisher(queueName);
      }

      /// <summary>
      /// Creates an instance of a receiver from Azure Storage Queues using development storage.
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="queueName">Queue name</param>
      /// <param name="messageVisibilityTimeout">Message visibility timeout</param>
      /// <returns>Generic message receiver interface</returns>
      public static IMessageReceiver AzureDevelopmentStorageQueueReceiver(this IMessagingFactory factory,
         string queueName,
         TimeSpan messageVisibilityTimeout)
      {
         return new AzureStorageQueueReceiver(queueName, messageVisibilityTimeout);
      }

      /// <summary>
      /// Creates an instance of a receiver from Azure Storage Queues using development storage.
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="queueName">Queue name</param>
      /// <param name="messageVisibilityTimeout">Message visibility timeout</param>
      /// <param name="messagePollingInterval">Storage Queues do not support listening therefore internally we poll for new messages. This parameters
      /// indicates how often this happens</param>
      /// <returns>Generic message receiver interface</returns>
      public static IMessageReceiver AzureDevelopmentStorageQueueReceiver(this IMessagingFactory factory,
         string queueName,
         TimeSpan messageVisibilityTimeout,
         TimeSpan messagePollingInterval)
      {
         return new AzureStorageQueueReceiver(queueName, messageVisibilityTimeout, messagePollingInterval);
      }
   }
}
