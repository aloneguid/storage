using System;
using Storage.Net.Blob;
using Storage.Net.Messaging;
using Storage.Net.Microsoft.Azure.Storage.Blob;
using Storage.Net.Microsoft.Azure.Storage.Messaging;
using Storage.Net.Microsoft.Azure.Storage.KeyValue;
using Storage.Net.KeyValue;
using System.Net;
using Storage.Net.Microsoft.Azure.Storage;

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
      /// <param name="containerName">Container name in the blob storage. If the container doesn't exist it will be automatically
      /// created for you.</param>
      /// <returns>Generic blob storage interface</returns>
      public static IBlobStorage AzureBlobStorage(this IBlobStorageFactory factory,
         string accountName,
         string key,
         string containerName)
      {
         return new AzureUniversalBlobStorageProvider(accountName, key, containerName);
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
         return new AzureUniversalBlobStorageProvider(accountName, key);
      }

      /// <summary>
      /// Creates a blob storage implementation based on Microsoft Azure Blob Storage using a SAS UEI
      /// bound to a container.
      /// </summary>
      public static IBlobStorage AzureBlobStorageByContainerSasUri(this IBlobStorageFactory factory,
         Uri sasUri)
      {
         return AzureUniversalBlobStorageProvider.CreateWithContainerSasUri(sasUri);
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

   }
}
