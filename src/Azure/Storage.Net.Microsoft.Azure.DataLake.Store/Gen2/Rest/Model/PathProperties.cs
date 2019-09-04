using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Refit;
using NetBox.Extensions;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest.Model
{
   /// <summary>
   /// Path properties populated from the response headers.
   /// It's probably incomplete so if you need more refer to https://docs.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/path/getproperties#response
   /// </summary>
   class PathProperties
   {
      private static readonly char[] MetadataPairsSeparator = new[] { ',' };
      private static readonly char[] MetadataPairSeparator = new[] { '=' };

      public long? Length { get; internal set; }

      public DateTimeOffset? LastModified { get; internal set; }
      public string ETag { get; private set; }
      public string ContentType { get; }
      public string ResourceType { get; }

      public Dictionary<string, string> UserMetadata { get; }

      public PathProperties(ApiResponse<string> refitResponse)
      {
         try
         {
            Length = refitResponse.ContentHeaders.ContentLength;
         }
         catch(ObjectDisposedException)
         {
            //if content is not present, you can't access the length
         }
         LastModified = refitResponse.ContentHeaders.LastModified;
         ETag = refitResponse.Headers.ETag?.ToString();

         // for full set of headers see https://docs.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/path/getproperties#response

         ContentType = refitResponse.GetHeader("Content-Type");
         ResourceType = refitResponse.GetHeader("x-ms-resource-type");

         string um = refitResponse.GetHeader("x-ms-properties");
         if(um != null)
         {
            UserMetadata = um
               .Split(MetadataPairsSeparator, StringSplitOptions.RemoveEmptyEntries)
               .Select(pair => pair.Split(MetadataPairSeparator, 2))
               .ToDictionary(a => a[0], a => a[1].Base64Decode());
         }
      }
   }
}
