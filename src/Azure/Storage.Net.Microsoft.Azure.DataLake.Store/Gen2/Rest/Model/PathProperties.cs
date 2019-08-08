using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Refit;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest.Model
{
   /// <summary>
   /// Path properties populated from the response headers.
   /// It's probably incomplete so if you need more refer to https://docs.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/path/getproperties#response
   /// </summary>
   class PathProperties
   {
      public long? Length { get; internal set; }

      public DateTimeOffset? LastModified { get; internal set; }
      public string ETag { get; private set; }
      public string ContentType { get; }
      public string ResourceType { get; }

      public PathProperties(ApiResponse<string> refitResponse)
      {
         Length = refitResponse.ContentHeaders.ContentLength;
         LastModified = refitResponse.ContentHeaders.LastModified;
         ETag = refitResponse.Headers.ETag?.ToString();

         // for full set of headers see https://docs.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/path/getproperties#response

         ContentType = GetHeader(refitResponse, "Content-Type");
         ResourceType = GetHeader(refitResponse, "x-ms-resource-type");
      }

      private static string GetHeader(ApiResponse<string> refitResponse, string name)
      {
         if(!refitResponse.Headers.TryGetValues(name, out IEnumerable<string> values))
            return null;

         return values.First();
      }
   }
}
