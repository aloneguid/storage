using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.Microsoft.Azure.Storage.Files;

namespace Storage.Net
{
   /// <summary>
   /// Factory
   /// </summary>
   public static class AzureFilesFactory
   {
      /// <summary>
      /// Register Azure module.
      /// </summary>
      public static IModulesFactory UseAzureFilesStorage(this IModulesFactory factory)
      {
         return factory.Use(new Module());
      }

      /// <summary>
      /// Creates a blob storage implementation based on Microsoft Azure Files.
      /// </summary>
      /// <param name="factory">Reference to factory</param>
      /// <param name="accountName">Storage Account name</param>
      /// <param name="key">Storage Account key</param>
      /// <returns>Generic blob storage interface</returns>
      public static IBlobStorage AzureFiles(this IBlobStorageFactory factory,
         string accountName,
         string key)
      {
         return AzureFilesBlobStorage.CreateFromAccountNameAndKey(accountName, key);
      }

      /// <summary>
      /// Create connection string for azure blob storage
      /// </summary>
      public static StorageConnectionString ForAzureFilesStorageWithSharedKey(this IConnectionStringFactory factory,
         string accountName,
         string accountKey)
      {
         var cs = new StorageConnectionString(KnownPrefix.AzureFilesStorage);
         cs.Parameters[KnownParameter.AccountName] = accountName;
         cs.Parameters[KnownParameter.KeyOrPassword] = accountKey;
         return cs;
      }
   }
}
