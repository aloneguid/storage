using Microsoft.Azure.Storage.Blob;
using NetBox.Extensions;
using Storage.Net.Blobs;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   static class AzConvert
   {
      public static Blob ToBlob(CloudBlobContainer container)
      {
         var blob = new Blob(container.Name, BlobItemKind.Folder);
         blob.Properties["IsContainer"] = "True";
         
         if(container.Properties != null)
         {
            BlobContainerProperties props = container.Properties;
            if(props.ETag != null)
               blob.Properties["ETag"] = props.ETag;
            if(props.HasImmutabilityPolicy != null)
               blob.Properties["HasImmutabilityPolicy"] = props.HasImmutabilityPolicy.ToString();
            if(props.HasLegalHold != null)
               blob.Properties["HasLegalHold"] = props.HasLegalHold.ToString();
            blob.Properties["LeaseDuration"] = props.LeaseDuration.ToString();
            blob.Properties["LeaseState"] = props.LeaseState.ToString();
            blob.Properties["LeaseStatus"] = props.LeaseStatus.ToString();
            if(props.PublicAccess != null)
               blob.Properties["PublicAccess"] = props.PublicAccess.ToString();
         }

         if(container.Metadata?.Count > 0)
         {
            blob.Metadata.MergeRange(container.Metadata);
         }
         
         return blob;
      }
   }
}
