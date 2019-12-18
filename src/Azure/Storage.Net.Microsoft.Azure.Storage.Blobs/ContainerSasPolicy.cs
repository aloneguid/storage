using System;
using Azure.Storage;
using Azure.Storage.Sas;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   /// <summary>
   /// Defines a SAS policy for a container
   /// </summary>
   public class ContainerSasPolicy : OffsetSasPolicy
   {
      /// <summary>
      /// 
      /// </summary>
      /// <param name="startTime"></param>
      /// <param name="duration"></param>
      public ContainerSasPolicy(DateTimeOffset startTime, TimeSpan duration) : base(startTime, duration)
      {

      }

      /// <summary>
      /// Permissions granted
      /// </summary>
      public ContainerSasPermission Permissions { get; set; } = ContainerSasPermission.List | ContainerSasPermission.Read;

      internal string ToSasQuery(StorageSharedKeyCredential sasSigningCredentials, string containerName)
      {
         if(sasSigningCredentials is null)
            throw new ArgumentNullException(nameof(sasSigningCredentials));

         var sas = new BlobSasBuilder
         {
            BlobContainerName = containerName,
            Protocol = SasProtocol.Https,
            StartsOn = StartTime,
            ExpiresOn = ExpiryTime
         };

         sas.SetPermissions((BlobContainerSasPermissions)(int)Permissions);

         string query = sas.ToSasQueryParameters(sasSigningCredentials).ToString();
         return query;
      }
   }
}
