using System;
using Azure.Core;
using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Storage.Net.ConnectionString;
using Storage.Net.Microsoft.Azure.Storage.Blobs;

namespace Storage.Net
{
   /// <summary>
   /// Blob storage factory
   /// </summary>
   public static class Factory
   {
      /// <summary>
      /// Register Azure module.
      /// </summary>
      public static IModulesFactory UseAzureBlobStorage(this IModulesFactory factory)
      {
         return factory.Use(new Module());
      }

      /// <summary>
      /// Connect to local emulator
      /// </summary>
      public static IAzureBlobStorage AzureBlobStorageWithLocalEmulator(this IBlobStorageFactory factory)
      {
         var credential = new StorageSharedKeyCredential(
            "devstoreaccount1",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");

         var client = new BlobServiceClient(
            new Uri("http://127.0.0.1:10000/devstoreaccount1"),
            credential);

         return new AzureBlobStorage(client, "devstoreaccount1", credential);
      }

      /// <summary>
      /// 
      /// </summary>
      public static IAzureBlobStorage AzureBlobStorageWithSharedKey(this IBlobStorageFactory factory,
         string accountName,
         string key,
         Uri serviceUri = null)
      {
         if(accountName is null)
            throw new ArgumentNullException(nameof(accountName));
         if(key is null)
            throw new ArgumentNullException(nameof(key));

         var credential = new StorageSharedKeyCredential(accountName, key);

         var client = new BlobServiceClient(serviceUri ?? GetServiceUri(accountName), credential);

         return new AzureBlobStorage(client, accountName, credential);
      }

      /// <summary>
      /// 
      /// </summary>
      public static IAzureDataLakeStorage AzureDataLakeStorageWithSharedKey(this IBlobStorageFactory factory,
         string accountName,
         string key,
         Uri serviceUri = null)
      {
         if(accountName is null)
            throw new ArgumentNullException(nameof(accountName));
         if(key is null)
            throw new ArgumentNullException(nameof(key));

         var credential = new StorageSharedKeyCredential(accountName, key);

         var client = new BlobServiceClient(serviceUri ?? GetServiceUri(accountName), credential);

         return new AzureDataLakeStorage(client, accountName, credential);
      }

      /// <summary>
      /// Create Azure Blob Storage with AAD authentication
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="accountName"></param>
      /// <param name="tenantId"></param>
      /// <param name="applicationId"></param>
      /// <param name="applicationSecret"></param>
      /// <param name="activeDirectoryAuthEndpoint"></param>
      /// <returns></returns>
      public static IAzureBlobStorage AzureBlobStorageWithAzureAd(this IBlobStorageFactory factory,
         string accountName,
         string tenantId,
         string applicationId,
         string applicationSecret,
         string activeDirectoryAuthEndpoint = "https://login.microsoftonline.com/")
      {
         if(accountName is null)
            throw new ArgumentNullException(nameof(accountName));
         if(tenantId is null)
            throw new ArgumentNullException(nameof(tenantId));
         if(applicationId is null)
            throw new ArgumentNullException(nameof(applicationId));
         if(applicationSecret is null)
            throw new ArgumentNullException(nameof(applicationSecret));
         if(activeDirectoryAuthEndpoint is null)
            throw new ArgumentNullException(nameof(activeDirectoryAuthEndpoint));

         // Create a token credential that can use our Azure Active
         // Directory application to authenticate with Azure Storage
         TokenCredential credential =
             new ClientSecretCredential(
                 tenantId,
                 applicationId,
                 applicationSecret,
                 new TokenCredentialOptions() { AuthorityHost = new Uri(activeDirectoryAuthEndpoint) });

         // Create a client that can authenticate using our token credential
         var client = new BlobServiceClient(GetServiceUri(accountName), credential);

         return new AzureBlobStorage(client, accountName);
      }

      /// <summary>
      /// Create Azure Data Lake Gen 2 Storage with AAD authentication
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="accountName"></param>
      /// <param name="tenantId"></param>
      /// <param name="applicationId"></param>
      /// <param name="applicationSecret"></param>
      /// <param name="activeDirectoryAuthEndpoint"></param>
      /// <returns></returns>
      public static IAzureDataLakeStorage AzureDataLakeStorageWithAzureAd(this IBlobStorageFactory factory,
         string accountName,
         string tenantId,
         string applicationId,
         string applicationSecret,
         string activeDirectoryAuthEndpoint = "https://login.microsoftonline.com/")
      {
         if(accountName is null)
            throw new ArgumentNullException(nameof(accountName));
         if(tenantId is null)
            throw new ArgumentNullException(nameof(tenantId));
         if(applicationId is null)
            throw new ArgumentNullException(nameof(applicationId));
         if(applicationSecret is null)
            throw new ArgumentNullException(nameof(applicationSecret));
         if(activeDirectoryAuthEndpoint is null)
            throw new ArgumentNullException(nameof(activeDirectoryAuthEndpoint));

         // Create a token credential that can use our Azure Active
         // Directory application to authenticate with Azure Storage
         TokenCredential credential =
             new ClientSecretCredential(
                 tenantId,
                 applicationId,
                 applicationSecret,
                 new TokenCredentialOptions() { AuthorityHost = new Uri(activeDirectoryAuthEndpoint) });

         // Create a client that can authenticate using our token credential
         var client = new BlobServiceClient(GetServiceUri(accountName), credential);

         return new AzureDataLakeStorage(client, accountName);
      }

      /// <summary>
      /// 
      /// </summary>
      public static IAzureBlobStorage AzureBlobStorageWithTokenCredential(this IBlobStorageFactory factory,
         string accountName,
         TokenCredential tokenCredential)
      {
         var client = new BlobServiceClient(GetServiceUri(accountName), tokenCredential);

         return new AzureBlobStorage(client, accountName);
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="sas"></param>
      /// <returns></returns>
      public static IAzureBlobStorage AzureBlobStorageWithSas(this IBlobStorageFactory factory,
         string sas)
      {
         TryParseSasUrl(sas, out string accountName, out string containerName, out string sasQuery);

         var client = new BlobServiceClient(new Uri(sas));

         return new AzureBlobStorage(client, accountName, containerName: containerName);
      }

      /// <summary>
      /// Creates Azure Blob Storage with Managed Identity
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="accountName"></param>
      /// <param name="clientId"></param>
      /// <returns></returns>
      public static IAzureBlobStorage AzureBlobStorageWithMsi(this IBlobStorageFactory factory,
         string accountName,
         string clientId = null)
      {
         TokenCredential credential = new ManagedIdentityCredential(clientId, null);

         var client = new BlobServiceClient(GetServiceUri(accountName), credential);

         return new AzureBlobStorage(client, accountName);
      }



      /// <summary>
      /// Creates Azure Data Lake Gen 2 Storage with Managed Identity
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="accountName"></param>
      /// <param name="clientId"></param>
      /// <returns></returns>
      public static IAzureDataLakeStorage AzureDataLakeStorageWithMsi(this IBlobStorageFactory factory,
         string accountName,
         string clientId = null)
      {
         TokenCredential credential = new ManagedIdentityCredential(clientId, null);

         var client = new BlobServiceClient(GetServiceUri(accountName), credential);

         return new AzureDataLakeStorage(client, accountName);
      }


      /*
      /// <summary>
      /// Creates a blob storage implementation based on Microsoft Azure Blob Storage using development storage.
      /// </summary>
      /// <param name="factory">Reference to factory</param>
      /// <returns>Generic blob storage interface</returns>
      public static IAzureBlobStorage AzureBlobDevelopmentStorage(this IBlobStorageFactory factory)
      {
         return AzureUniversalBlobStorageProvider.CreateForLocalEmulator();
      }
      }
      */

      /// <summary>
      /// Create connection string for azure blob storage
      /// </summary>
      public static StorageConnectionString ForAzureBlobStorageWithSharedKey(this IConnectionStringFactory factory,
         string accountName,
         string accountKey)
      {
         var cs = new StorageConnectionString(KnownPrefix.AzureBlobStorage);
         cs.Parameters[KnownParameter.AccountName] = accountName;
         cs.Parameters[KnownParameter.KeyOrPassword] = accountKey;
         return cs;
      }

      /// <summary>
      /// Create connection string for azure blob storage
      /// </summary>
      public static StorageConnectionString ForAzureDataLakeStorageWithSharedKey(this IConnectionStringFactory factory,
         string accountName,
         string accountKey)
      {
         var cs = new StorageConnectionString(KnownPrefix.AzureDataLakeGen2);
         cs.Parameters[KnownParameter.AccountName] = accountName;
         cs.Parameters[KnownParameter.KeyOrPassword] = accountKey;
         return cs;
      }

      /// <summary>
      /// 
      /// </summary>
      public static StorageConnectionString ForAzureBlobStorageWithAzureAd(this IConnectionStringFactory factory,
         string accountName,
         string tenantId,
         string applicationId,
         string applicationSecret)
      {
         var cs = new StorageConnectionString(KnownPrefix.AzureBlobStorage);
         cs.Parameters[KnownParameter.AccountName] = accountName;
         cs.Parameters[KnownParameter.TenantId] = tenantId;
         cs.Parameters[KnownParameter.ClientId] = applicationId;
         cs.Parameters[KnownParameter.ClientSecret] = applicationSecret;
         return cs;
      }

      /// <summary>
      /// 
      /// </summary>
      public static StorageConnectionString ForAzureDataLakeStorageWithAzureAd(this IConnectionStringFactory factory,
         string accountName,
         string tenantId,
         string applicationId,
         string applicationSecret)
      {
         var cs = new StorageConnectionString(KnownPrefix.AzureDataLakeGen2);
         cs.Parameters[KnownParameter.AccountName] = accountName;
         cs.Parameters[KnownParameter.TenantId] = tenantId;
         cs.Parameters[KnownParameter.ClientId] = applicationId;
         cs.Parameters[KnownParameter.ClientSecret] = applicationSecret;
         return cs;
      }

      private static Uri GetServiceUri(string accountName)
      {
         return new Uri($"https://{accountName}.blob.core.windows.net/");
      }

      private static bool TryParseSasUrl(string url, out string accountName, out string containerName, out string sas)
      {
         try
         {
            var u = new Uri(url);

            accountName = u.Host.Substring(0, u.Host.IndexOf('.'));
            containerName = u.Segments.Length == 2 ? u.Segments[1] : null;
            sas = u.Query;

            return true;
         }
         catch
         {
            accountName = null;
            containerName = null;
            sas = null;
            return false;
         }

      }
   }
}
