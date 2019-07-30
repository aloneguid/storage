using System;
using System.Collections.Generic;
using System.Text;
using Objects = Google.Apis.Storage.v1.Data.Objects;
using Object = Google.Apis.Storage.v1.Data.Object;
using Storage.Net.Blobs;
using NetBox.Extensions;
using Google.Api.Gax;
using System.Threading.Tasks;

namespace Storage.Net.Gcp.CloudStorage.Blobs
{
   static class GConvert
   {
      public static Blob ToBlob(Object go)
      {
         var blob = new Blob(go.Name)
         {
            LastModificationTime = go.Updated,
            MD5 = go.Md5Hash,
            Size = (long?)go.Size,
         };

         if(go.Metadata?.Count > 0)
            blob.Metadata.AddRange(go.Metadata);

         if(go.ContentType != null)
            blob.Properties["ContentType"] = go.ContentType;

         return blob;
      }

      public static async Task<IReadOnlyCollection<Blob>> ToBlobsAsync(PagedAsyncEnumerable<Objects, Object> pae)
      {
         var result = new List<Blob>();

         using(IAsyncEnumerator<Object> enumerator = pae.GetEnumerator())
         {
            while(await enumerator.MoveNext())
            {
               Object go = enumerator.Current;

               result.Add(ToBlob(go));
            }
         }

         return result;
      }
   }
}
