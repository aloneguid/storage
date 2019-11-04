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
      /// Create Azure Event Hub publisher by full connection string
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="connectionString">Full connection string, including entity path</param>
      public static IMessenger AzureEventHub(this IMessagingFactory factory, string connectionString)
      {
         return new AzureEventHubMessenger(connectionString);
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
