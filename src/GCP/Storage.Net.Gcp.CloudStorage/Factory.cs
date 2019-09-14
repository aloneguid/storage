using Google.Apis.Auth.OAuth2;
using Storage.Net.Blobs;
using Storage.Net.Gcp.CloudStorage;
using Storage.Net.Gcp.CloudStorage.Blobs;
using NetBox.Extensions;

namespace Storage.Net
{
   /// <summary>
   /// Factory methods
   /// </summary>
   public static class Factory
   {
      /// <summary>
      /// Initialises Google Cloud Storage module required for connection strings to work
      /// </summary>
      /// <param name="factory"></param>
      /// <returns></returns>
      public static IModulesFactory UseGoogleCloudStorage(this IModulesFactory factory)
      {
         return factory.Use(new Module());
      }

      /// <summary>
      /// Creates a Google Cloud Storage storage instance, where credentials have to be configured
      /// in an environment variable as officially described at https://cloud.google.com/storage/docs/reference/libraries#setting_up_authentication
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="bucketName">Storage bucket name</param>
      /// <returns></returns>
      public static IBlobStorage GoogleCloudStorageFromEnvironmentVariable(this IBlobStorageFactory factory,
         string bucketName)
      {
         return new GoogleCloudStorageBlobStorage(bucketName);
      }

      /// <summary>
      /// Creates a Google Cloud Storage storage instance, where credentials are located in an external json file.
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="bucketName"></param>
      /// <param name="credentialsFilePath">Path to a json file containing credentials.</param>
      /// <returns></returns>
      public static IBlobStorage GoogleCloudStorageFromJsonFile(this IBlobStorageFactory factory,
         string bucketName,
         string credentialsFilePath)
      {
         GoogleCredential cred = GoogleCredential.FromFile(credentialsFilePath);

         return new GoogleCloudStorageBlobStorage(bucketName, cred);
      }

      /// <summary>
      /// Creates a Google Cloud Storage storage instance, where credentials are passed as a json string
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="bucketName"></param>
      /// <param name="credentialsJsonString">Json string containing credentials.</param>
      /// <param name="isBase64EncodedString">When true, <paramref name="credentialsJsonString"/> is bas64 encoded</param>
      /// <returns></returns>
      public static IBlobStorage GoogleCloudStorageFromJson(this IBlobStorageFactory factory,
         string bucketName,
         string credentialsJsonString,
         bool isBase64EncodedString = false)
      {
         string json = isBase64EncodedString ? credentialsJsonString.Base64Decode() : credentialsJsonString;

         GoogleCredential cred = GoogleCredential.FromJson(json);

         return new GoogleCloudStorageBlobStorage(bucketName, cred);
      }
   }
}
