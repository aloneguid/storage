﻿using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Refit;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest
{
   static class DataLakeApiFactory
   {
      public static IDataLakeApi CreateApiWithSharedKey(string accountName, string sharedKey)
      {
         return RestService.For<IDataLakeApi>(
            new HttpClient(new SharedSignatureHttpClientHandler(accountName, sharedKey))
            {
               BaseAddress = GetBaseAddress(accountName)
            });
      }

      public static IDataLakeApi CreateApiWithServicePrincipal(string accountName, string tenantId, string clientId, string clientSecret)
      {
         string baseUrl = $"https://{accountName}.dfs.core.windows.net";

         return RestService.For<IDataLakeApi>(
            new HttpClient(new ActiveDirectoryHttpClientHandler(accountName, tenantId, clientId, clientSecret))
            {
               BaseAddress = GetBaseAddress(accountName)
            });
      }

      private static Uri GetBaseAddress(string accountName) => new Uri($"https://{accountName}.dfs.core.windows.net");

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

         //event when content length is present but length is 0 the signature wants is to be an empty string
         string contentLengthValue = contentLength == null
            ? string.Empty
            : contentLength > 0
               ? contentLength.ToString()
               : string.Empty;

         return httpVerb + "\n" +
                contentEncoding + "\n" +
                contentLanguage + "\n" +
                contentLengthValue + "\n" +
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
         private readonly string _accountName;
         private readonly string _sharedKey;

         public SharedSignatureHttpClientHandler(string accountName, string sharedKey)
         {
            _accountName = accountName;
            _sharedKey = sharedKey;
         }

         private string CreateCanonicalisedResource(HttpRequestMessage request)
         {
            NameValueCollection coll = HttpUtility.ParseQueryString(request.RequestUri.Query);

            string headersResource = string.Join("\n",
               coll
               .Cast<string>()
               .OrderBy(key => key)
               .Select(key => $"{key}:{coll[key]}"));

            if(headersResource.Length > 0)
            {
               headersResource = "\n" + headersResource;
            }

            return $"/{_accountName}{request.RequestUri.LocalPath}{headersResource}";
         }

         protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
         {
            string dateHeader = DateTime.Now.ToString("R");
            string canonicalisedHeaders = $"x-ms-date:{dateHeader}\nx-ms-version:2018-11-09";
            string canonicalisedResource = CreateCanonicalisedResource(request);

            string signature = DataLakeApiFactory.GenerateSignature(
               httpVerb: request.Method.Method,
               contentLength: request.Content?.Headers?.ContentLength,
               range: request.Headers?.Range?.ToString(),
               canonicalisedHeaders: canonicalisedHeaders,
               canonicalisedResource: canonicalisedResource);

            request.Headers.Add("x-ms-date", dateHeader);
            request.Headers.Add("x-ms-version", "2018-11-09");

            using(var sha256 = new HMACSHA256(Convert.FromBase64String(_sharedKey)))
            {
               string hashed = Convert.ToBase64String(sha256.ComputeHash(Encoding.UTF8.GetBytes(signature)));
               var authHeader = new AuthenticationHeaderValue("SharedKey", _accountName + ":" + hashed);
               request.Headers.Authorization = authHeader;
            }

            return base.SendAsync(request, cancellationToken);
         }
      }

      private class ActiveDirectoryHttpClientHandler : HttpClientHandler
      {
         private const string Resource = "https://storage.azure.com/";
         private readonly AuthenticationContext _context;
         private readonly ClientCredential _credential;

         public ActiveDirectoryHttpClientHandler(string accountName,
            string tenantId,
            string clientId,
            string clientSecret)
         {
            string authority = $"https://login.microsoftonline.com/{tenantId}";
            _credential = new ClientCredential(clientId, clientSecret);
            _context = new AuthenticationContext(authority);
         }

         protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
         {
            AuthenticationResult authenticationResult = await _context.AcquireTokenAsync(Resource, _credential);
            var authHeader = new AuthenticationHeaderValue("Bearer", authenticationResult.AccessToken);
            request.Headers.Authorization = authHeader;
            return await base.SendAsync(request, cancellationToken);
         }
      }
   }
}
