using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Gen2.Interfaces;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Models;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Wrappers;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Gen2.BLL
{
   class DataLakeGen2RestApi : IDataLakeGen2RestApi
   {
      private const string ApiVersion = "2018-11-09";
      private readonly IAuthorisation _authorisation;
      private readonly string _baseUri;
      private readonly IHttpClientWrapper _httpClient;
      private readonly string _storageAccountName;

      public DataLakeGen2RestApi(HttpClient httpClient, IAuthorisation authorisation, string storageAccountName) :
         this(new HttpClientWrapper(httpClient), authorisation, storageAccountName)
      {
      }

      public DataLakeGen2RestApi(IHttpClientWrapper httpClient, IAuthorisation authorisation,
         string storageAccountName)
      {
         _httpClient = httpClient;
         _authorisation = authorisation;
         _storageAccountName = storageAccountName;
         _baseUri = $"https://{_storageAccountName}.dfs.core.windows.net";
      }

      public Task<HttpResponseMessage> AppendPathAsync(string filesystem, string path, byte[] content,
         long position, CancellationToken cancellationToken = default)
      {
         string uriPath = $"{filesystem}/{path}";
         var query = new Dictionary<string, string>
         {
            {"action", "append"},
            {"position", position.ToString()},
            {"timeout", "60"}
         };

         return SendAsync(new HttpMethod("PATCH"), uriPath, query, new ByteArrayContent(content),
            new Dictionary<string, string>(), cancellationToken);
      }

      public Task<HttpResponseMessage> CreateDirectoryAsync(string filesystem, string directory,
         CancellationToken cancellationToken = default)
      {
         string uriPath = $"{filesystem}/{directory}";
         var query = new Dictionary<string, string>
         {
            {"resource", "directory"},
            {"timeout", "60"}
         };

         return SendAsync(HttpMethod.Put, uriPath, query, new ByteArrayContent(new byte[0]),
            new Dictionary<string, string>(), cancellationToken);
      }

      public Task<HttpResponseMessage> CreateFileAsync(string filesystem, string path,
         CancellationToken cancellationToken = default)
      {
         string uriPath = $"{filesystem}/{path}";
         var query = new Dictionary<string, string>
         {
            {"resource", "file"},
            {"timeout", "60"}
         };

         return SendAsync(HttpMethod.Put, uriPath, query, new ByteArrayContent(new byte[0]),
            new Dictionary<string, string>(), cancellationToken);
      }

      public Task<HttpResponseMessage> CreateFilesystemAsync(string filesystem,
         CancellationToken cancellationToken = default)
      {
         string uriPath = filesystem;
         var query = new Dictionary<string, string>
         {
            {"resource", "filesystem"},
            {"timeout", "60"}
         };

         return SendAsync(HttpMethod.Put, uriPath, query, new ByteArrayContent(new byte[0]),
            new Dictionary<string, string>(), cancellationToken);
      }

      public Task<HttpResponseMessage> DeleteFilesystemAsync(string filesystem,
         CancellationToken cancellationToken = default)
      {
         string uriPath = filesystem;
         var query = new Dictionary<string, string>
         {
            {"resource", "filesystem"},
            {"timeout", "60"}
         };

         return SendAsync(HttpMethod.Delete, uriPath, query, new ByteArrayContent(new byte[0]),
            new Dictionary<string, string>(), cancellationToken);
      }

      public Task<HttpResponseMessage> DeletePathAsync(string filesystem, string path, bool isRecursive,
         CancellationToken cancellationToken = default)
      {
         string uriPath = $"{filesystem}/{path}";
         var query = new Dictionary<string, string>
         {
            {"recursive", isRecursive.ToString().ToLower()},
            {"timeout", "60"}
         };

         return SendAsync(HttpMethod.Delete, uriPath, query, new ByteArrayContent(new byte[0]),
            new Dictionary<string, string>(), cancellationToken);
      }

      public Task<HttpResponseMessage> GetAccessControlAsync(string filesystem, string path,
         CancellationToken cancellationToken = default)
      {
         string uriPath = $"{filesystem}/{path}";
         var query = new Dictionary<string, string>
         {
            {"action", "getaccesscontrol"},
            {"upn", "true"},
            {"timeout", "60"}
         };

         return SendAsync(HttpMethod.Head, uriPath, query, new ByteArrayContent(new byte[0]),
            new Dictionary<string, string>(), cancellationToken);
      }

      public Task<HttpResponseMessage> GetStatusAsync(string filesystem, string path,
         CancellationToken cancellationToken = default)
      {
         string uriPath = $"{filesystem}/{path}";
         var query = new Dictionary<string, string>
         {
            {"timeout", "60"}
         };

         return SendAsync(HttpMethod.Head, uriPath, query, new ByteArrayContent(new byte[0]),
            new Dictionary<string, string>(), cancellationToken);
      }

      public Task<HttpResponseMessage> FlushPathAsync(string filesystem, string path, long position,
         CancellationToken cancellationToken = default)
      {
         string uriPath = $"{filesystem}/{path}";
         var query = new Dictionary<string, string>
         {
            {"action", "flush"},
            {"position", position.ToString()},
            {"timeout", "60"}
         };

         return SendAsync(new HttpMethod("PATCH"), uriPath, query, new ByteArrayContent(new byte[0]),
            new Dictionary<string, string>(), cancellationToken);
      }

      public Task<HttpResponseMessage> ListFilesystemsAsync(int maxResults, CancellationToken cancellationToken = default)
      {
         var query = new Dictionary<string, string>
         {
            {"maxresults", maxResults.ToString()},
            {"resource", "account"},
            {"timeout", "60"}
         };

         return SendAsync(HttpMethod.Get, "", query, new ByteArrayContent(new byte[0]),
            new Dictionary<string, string>(), cancellationToken);
      }

      public Task<HttpResponseMessage> ListPathAsync(string filesystem, string directory, bool isRecursive,
         int maxResults, CancellationToken cancellationToken = default)
      {
         string uriPath = filesystem;
         var query = new Dictionary<string, string>
         {
            {"directory", directory},
            {"maxresults", maxResults.ToString()},
            {"recursive", isRecursive.ToString().ToLower()},
            {"resource", "filesystem"},
            {"timeout", "60"}
         };

         return SendAsync(HttpMethod.Get, uriPath, query, new ByteArrayContent(new byte[0]),
            new Dictionary<string, string>(), cancellationToken);
      }

      public Task<HttpResponseMessage> ReadPathAsync(string filesystem, string path, long? start = null,
         long? end = null, CancellationToken cancellationToken = default)
      {
         string uriPath = $"{filesystem}/{path}";
         var query = new Dictionary<string, string>
         {
            {"timeout", "60"}
         };

         var headers = new Dictionary<string, string>();

         if(start != null && end != null)
         {
            headers.Add("range", $"bytes={start.Value}-{end.Value}");
         }

         return SendAsync(HttpMethod.Get, uriPath, query, new ByteArrayContent(new byte[0]), headers,
            cancellationToken);
      }

      public Task<HttpResponseMessage> SetAccessControlAsync(string filesystem, string path, string acl,
         CancellationToken cancellationToken = default)
      {
         string uriPath = $"{filesystem}/{path}";
         var query = new Dictionary<string, string>
         {
            {"action", "setaccesscontrol"},
            {"timeout", "60"}
         };

         var headers = new Dictionary<string, string>
         {
            {"x-ms-acl", acl}
         };

         return SendAsync(new HttpMethod("PATCH"), uriPath, query, new ByteArrayContent(new byte[0]), headers,
            cancellationToken);
      }

      private async Task<HttpResponseMessage> SendAsync(
         HttpMethod method,
         string path,
         IDictionary<string, string> query,
         HttpContent content,
         IDictionary<string, string> headers,
         CancellationToken cancellationToken)
      {
         string dateTimeReference = DateTime.Now.ToString("R");

         headers.Add("x-ms-date", dateTimeReference);
         headers.Add("x-ms-version", ApiVersion);

         string canonicalisedHeaders = CreateCanonicalisedHeaders(headers);
         string canonicalisedResources = CreateCanonicalisedResources(query);
         string queryString = CreateQueryString(query);
         var uri = new Uri($"{_baseUri}/{path}?{queryString}");

         string signature = GenerateSignature(method.Method,
            contentLength: content.Headers.ContentLength > 0 ? content.Headers.ContentLength.ToString() : "",
            range: headers.ContainsKey("range") ? headers["range"] : "",
            canonicalisedHeaders: canonicalisedHeaders,
            canonicalisedResource: $"/{_storageAccountName}{uri.AbsolutePath}\n{canonicalisedResources}"
         );

         var request = new HttpRequestMessage
         {
            Method = method,
            Content = content,
            RequestUri = uri,
            Headers = {Authorization = await _authorisation.AuthoriseAsync(_storageAccountName, signature)}
         };

         headers.ToList().ForEach(x => request.Headers.Add(x.Key, x.Value));

         HttpResponseMessage result = await _httpClient.SendAsync(request, cancellationToken);

         try
         {
            result.EnsureSuccessStatusCode();
         }
         catch(Exception e)
         {
            throw new DataLakeGen2Exception("Exception occurred during call to Data Lake Gen 2 Rest API.", e)
            {
               StatusCode = result.StatusCode
            };
         }

         return result;
      }

      private static string GenerateSignature(
         string httpVerb,
         string contentEncoding = "",
         string contentLanguage = "",
         string contentLength = "",
         string contentMd5 = "",
         string contentType = "",
         string date = "",
         string ifModifiedSince = "",
         string ifMatch = "",
         string ifNoneMatch = "",
         string ifUnmodifiedSince = "",
         string range = "",
         string canonicalisedHeaders = "",
         string canonicalisedResource = ""
      )
      {
         return httpVerb + "\n" +
                contentEncoding + "\n" +
                contentLanguage + "\n" +
                contentLength + "\n" +
                contentMd5 + "\n" +
                contentType + "\n" +
                date + "\n" +
                ifModifiedSince + "\n" +
                ifMatch + "\n" +
                ifNoneMatch + "\n" +
                ifUnmodifiedSince + "\n" +
                range + "\n" +
                canonicalisedHeaders + "\n" +
                canonicalisedResource;
      }

      private static string CreateCanonicalisedHeaders(IDictionary<string, string> headers)
      {
         return string.Join("\n", headers
            .Where(x => x.Key.StartsWith("x-ms-"))
            .OrderBy(x => x.Key)
            .Select(x => $"{x.Key}:{x.Value}"));
      }

      private static string CreateCanonicalisedResources(IDictionary<string, string> query)
      {
         return string.Join("\n", query
            .OrderBy(x => x.Key)
            .Select(x => $"{x.Key}:{x.Value}"));
      }

      private static string CreateQueryString(IDictionary<string, string> query)
      {
         return string.Join("&", query
            .Select(x => $"{x.Key}={x.Value}")
            .ToArray());
      }
   }
}