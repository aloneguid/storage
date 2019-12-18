namespace Blobs
{
   static class Constants
   {
      /// <summary>
      /// Header Request Names
      /// </summary>
      internal static class HeaderNames
      {
         public const string XMsPrefix = "x-ms-";
         public const string MetadataPrefix = "x-ms-meta-";
         public const string ErrorCode = "x-ms-error-code";
         public const string RequestId = "x-ms-request-id";
         public const string ClientRequestId = "x-ms-client-request-id";
         public const string Date = "x-ms-date";
         public const string SharedKey = "SharedKey";
         public const string Authorization = "Authorization";
         public const string ContentEncoding = "Content-Encoding";
         public const string ContentLanguage = "Content-Language";
         public const string ContentLength = "Content-Length";
         public const string ContentMD5 = "Content-MD5";
         public const string ContentType = "Content-Type";
         public const string IfModifiedSince = "If-Modified-Since";
         public const string IfMatch = "If-Match";
         public const string IfNoneMatch = "If-None-Match";
         public const string IfUnmodifiedSince = "If-Unmodified-Since";
         public const string Range = "Range";
         public const string ContentRange = "Content-Range";
      }

   }
}
