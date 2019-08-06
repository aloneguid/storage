using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Refit;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest
{
   static class DataLakeApiFactory
   {
      public static IDataLakeApi CreateApi(string accountName, string sharedKey)
      {
         string baseUrl = $"https://{accountName}.dfs.core.windows.net";

         return RestService.For<IDataLakeApi>(
            new HttpClient(new SharedSignatureHttpClientHandler(sharedKey))
            {
               BaseAddress = new Uri(baseUrl)
            });
      }

      private static string GenerateSignature(
         string httpVerb,
         string contentEncoding = "",
         string contentLanguage = "",
         long? contentLength = null,
         string contentMd5 = "",
         string contentType = "",
         string date = "",
         string ifModifiedSince = "",
         string ifMatch = "",
         string ifNoneMatch = "",
         string ifUnmodifiedSince = "",
         string range = "",
         string canonicalisedHeaders = null,
         string canonicalisedResource = ""
      )
      {
         string canonicalHeaders = $"x-ms-date:{DateTime.UtcNow.ToString("R")}\nx-ms-version:2018-11-09";

         return httpVerb + "\n" +
                contentEncoding + "\n" +
                contentLanguage + "\n" +
                (contentLength?.ToString() ?? "") + "\n" +
                contentMd5 + "\n" +
                contentType + "\n" +
                date + "\n" +
                ifModifiedSince + "\n" +
                ifMatch + "\n" +
                ifNoneMatch + "\n" +
                ifUnmodifiedSince + "\n" +
                range + "\n" +
                (canonicalisedHeaders ?? canonicalHeaders) + "\n" +
                canonicalisedResource;
      }


      private class SharedSignatureHttpClientHandler : HttpClientHandler
      {
         private readonly string _sharedKey;

         public SharedSignatureHttpClientHandler(string sharedKey)
         {
            _sharedKey = sharedKey;
         }

         protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
         {
            string canonicalResource = "";

            string signature = DataLakeApiFactory.GenerateSignature(
               httpVerb: request.Method.ToString().ToUpper(),
               contentLength: request.Content.Headers.ContentLength,
               canonicalisedResource: canonicalResource);

            return base.SendAsync(request, cancellationToken);
         }
      }
   }
}
