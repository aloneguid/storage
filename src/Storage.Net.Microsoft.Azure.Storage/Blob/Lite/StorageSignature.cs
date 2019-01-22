using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Storage.Net.Microsoft.Azure.Storage.Blob.Lite
{
   class StorageSignature
   {
      private readonly StringBuilder _sb = new StringBuilder();

      public StorageSignature(
         HttpMethod method = null,
         string contentEncoding = null,
         string contentLanguage = null,
         string contentLength = null,
         string contentMd5 = null,
         string contentType = null,
         string date = null,
         string ifModifiedSince = null,
         string ifMatch = null,
         string ifNoneMatch = null,
         string ifUnmodifiedSince = null,
         string range = null)
      {
         if (method == null) method = HttpMethod.Get;
         Add(method.ToString().ToUpper());
         Add(contentEncoding);
         Add(contentLanguage);
         Add(contentLength);
         Add(contentMd5);
         Add(contentType);
         Add(date);
         Add(ifModifiedSince);
         Add(ifMatch);
         Add(ifNoneMatch);
         Add(ifUnmodifiedSince);
         Add(range);
      }

      private void Add(string value)
      {
         _sb.Append(value == null ? string.Empty : value);
         _sb.Append("\n");
      }
   }
}
