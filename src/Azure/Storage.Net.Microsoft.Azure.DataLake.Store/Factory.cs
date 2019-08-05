using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.DataLake.Store;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen1;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2;
using System;
using System.Net;

namespace Storage.Net
{
   /// <summary>
   /// Factory class that implement factory methods for Microsoft Azure implememtations
   /// </summary>
   public static class Factory
   {
      public static IModulesFactory UseAzureDataLake(this IModulesFactory factory)
      {
         return factory.Use(new Module());
      }

      /// <summary>
      /// Creates and instance of Azure Data Lake Store client
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="accountName">Data Lake account name</param>
      /// <param name="tenantId">Tenant ID</param>
      /// <param name="principalId">Principal ID</param>
      /// <param name="principalSecret">Principal Secret</param>
      /// <param name="listBatchSize">Batch size for list operation for this storage connection. If not set defaults to 5000.</param>
      /// <returns></returns>
      public static IBlobStorage AzureDataLakeGen1StoreByClientSecret(this IBlobStorageFactory factory,
         string accountName,
         string tenantId,
         string principalId,
         string principalSecret,
         int listBatchSize = 5000)
      {
         if (accountName == null)
            throw new ArgumentNullException(nameof(accountName));

         if (tenantId == null)
            throw new ArgumentNullException(nameof(tenantId));

         if (principalId == null)
            throw new ArgumentNullException(nameof(principalId));

         if (principalSecret == null)
            throw new ArgumentNullException(nameof(principalSecret));

         var client = AzureDataLakeGen1Storage.CreateByClientSecret(accountName, new NetworkCredential(principalId, principalSecret, tenantId));
         client.ListBatchSize = listBatchSize;
         return client;
      }

      /// <summary>
      /// Creates and instance of Azure Data Lake Gen 2 Store client
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="accountName">Data Lake account name</param>
      /// <param name="tenantId">Tenant ID</param>
      /// <param name="principalId">Principal ID</param>
      /// <param name="principalSecret">Principal Secret</param>
      /// <param name="listBatchSize">Batch size for list operation for this storage connection. If not set defaults to 5000.</param>
      /// <returns></returns>
      public static IBlobStorage AzureDataLakeGen2StoreByClientSecret(this IBlobStorageFactory factory,
         string accountName,
         string tenantId,
         string principalId,
         string principalSecret,
         int listBatchSize = 5000)
      {
         if(accountName == null)
            throw new ArgumentNullException(nameof(accountName));

         if(tenantId == null)
            throw new ArgumentNullException(nameof(tenantId));

         if(principalId == null)
            throw new ArgumentNullException(nameof(principalId));

         if(principalSecret == null)
            throw new ArgumentNullException(nameof(principalSecret));

         var client = AzureDataLakeStoreGen2BlobStorageProvider.CreateByClientSecret(accountName, new NetworkCredential(principalId, principalSecret, tenantId));
         client.ListBatchSize = listBatchSize;

         return client;
      }

      /// <summary>
      /// Creates and instance of Azure Data Lake Gen 2 Store client
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="accountName">Data Lake account name</param>
      /// <param name="accessKey">Shared access key</param>
      /// <param name="listBatchSize">Batch size for list operation for this storage connection. If not set defaults to 5000.</param>
      /// <returns></returns>
      public static IBlobStorage AzureDataLakeGen2StoreBySharedAccessKey(this IBlobStorageFactory factory,
         string accountName,
         string accessKey,
         int listBatchSize = 5000)
      {
         if(accountName == null)
            throw new ArgumentNullException(nameof(accountName));

         if(accessKey == null)
            throw new ArgumentNullException(nameof(accessKey));

         var client = AzureDataLakeStoreGen2BlobStorageProvider.CreateBySharedAccessKey(accountName, accessKey);
         client.ListBatchSize = listBatchSize;

         return client;
      }
   }
}
