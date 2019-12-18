using Storage.Net.Messaging;
using Storage.Net.Microsoft.Azure.EventHub;

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
      public static IModulesFactory UseAzureEventHubs(this IModulesFactory factory)
      {
         return factory.Use(new Module());
      }


      /// <summary>
      /// Create Azure Event Hub messenger by full connection string and provide all the information for receiving end.
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="connectionString">Full connection string, including entity path</param>
      /// <param name="azureBlobStorageConnectionString">Native Azure Blob Storage connection string. Event Hub receiver requires this for internal state management, therefore you need to provide it if you plan to receive messages, and not just send them.</param>
      /// <param name="consumerGroupName">Name of of the consumer group, defaults to "$Default" when not passed, however it's a good practive to create a new consumer group.</param>
      /// <param name="leaseContainerName">Name of the container to use for internal state, defaults to "eventhubs".</param>
      /// <param name="storageBlobPrefix">If you are planning to use the same container for multiple event hubs, you can pass an optional prefix here.</param>
      public static IMessenger AzureEventHub(this IMessagingFactory factory,
         string connectionString,
         string azureBlobStorageConnectionString = null,
         string consumerGroupName = null,
         string leaseContainerName = null,
         string storageBlobPrefix = null
         )
      {
         return new AzureEventHubMessenger(
            connectionString,
            azureBlobStorageConnectionString,
            consumerGroupName,
            leaseContainerName,
            storageBlobPrefix);
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="namespaceName"></param>
      /// <param name="entityName"></param>
      /// <param name="keyName"></param>
      /// <param name="key"></param>
      /// <returns></returns>
      public static IMessenger AzureEventHub(this IMessagingFactory factory,
         string namespaceName, string entityName, string keyName, string key)
      {
         return new AzureEventHubMessenger(namespaceName, entityName, keyName, key);
      }
   }
}
