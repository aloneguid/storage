using System;
using System.Collections.Generic;
using System.Text;
using Azure.Storage;
using Azure.Storage.Sas;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   /// <summary>
   /// Defines a SAS policy for a blob
   /// </summary>
   public class BlobSasPolicy : OffsetSasPolicy
   {
      /// <summary>
      /// 
      /// </summary>
      /// <param name="startTime"></param>
      /// <param name="duration"></param>
      public BlobSasPolicy(DateTimeOffset startTime, TimeSpan duration) : base(startTime, duration)
      {
      }

      /// <summary>
      /// Permissions for this sas policy
      /// </summary>
      public BlobSasPermission Permissions { get; set; } = BlobSasPermission.Read;

      internal string ToSasQuery(StorageSharedKeyCredential sasSigningCredentials, string containerName, string blobName)
      {
         if(sasSigningCredentials is null)
            throw new ArgumentNullException(nameof(sasSigningCredentials));

         var sas = new BlobSasBuilder
         {
            BlobContainerName = containerName,
            BlobName = blobName,
            Protocol = SasProtocol.Https,
            StartsOn = StartTime,
            ExpiresOn = ExpiryTime
         };

         sas.SetPermissions((BlobSasPermissions)(int)Permissions);

         string query = sas.ToSasQueryParameters(sasSigningCredentials).ToString();
         return query;
      }
   }
}
