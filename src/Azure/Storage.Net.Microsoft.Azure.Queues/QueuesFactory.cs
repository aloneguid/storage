using System;
using Storage.Net.ConnectionString;
using Storage.Net.Messaging;
using Storage.Net.Microsoft.Azure.Queues;

namespace Storage.Net
{
   public static class QueuesFactory
   {
      /// <summary>
      /// Register Azure module.
      /// </summary>
      public static IModulesFactory UseAzureQueues(this IModulesFactory factory)
      {
         return factory.Use(new Module());
      }

      /// <summary>
      /// Creates an instance of a publisher to Azure Storage Queues
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="accountName">Account name</param>
      /// <param name="storageKey">Storage key</param>
      /// <returns>Generic message publisher interface</returns>
      public static IMessenger AzureStorageQueue(this IMessagingFactory factory,
         string accountName,
         string storageKey)
      {
         return new AzureStorageQueueMessenger(accountName, storageKey);   //
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="accountName"></param>
      /// <param name="accountKey"></param>
      /// <returns></returns>
      public static StorageConnectionString ForAzureStorageQueuesWithSharedKey(
         this IConnectionStringFactory factory,
         string accountName,
         string accountKey)
      {
         var cs = new StorageConnectionString(KnownPrefix.AzureQueueStorage);
         cs.Parameters[KnownParameter.AccountName] = accountName;
         cs.Parameters[KnownParameter.KeyOrPassword] = accountKey;
         return cs;
      }
   }
}
