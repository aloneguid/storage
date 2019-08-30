using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Graph;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace Storage.Net.Microsoft.Azure.DataLake.Store
{
   /// <summary>
   /// 
   /// </summary>
   public class GraphService
   {
      private const string Authority = "https://login.microsoftonline.com/";
      private const string ResourceUri = "https://graph.microsoft.com";

      private readonly string _tenantId;
      private readonly string _clientId;
      private readonly string _clientSecret;
      private readonly GraphServiceClient _client;

      /// <summary>
      /// 
      /// </summary>
      /// <param name="tenantId"></param>
      /// <param name="clientId"></param>
      /// <param name="clientSecret"></param>
      public GraphService(string tenantId, string clientId, string clientSecret)
      {
         _tenantId = tenantId;
         _clientId = clientId;
         _clientSecret = clientSecret;
         _client = new GraphServiceClient(new DelegateAuthenticationProvider(AuthenticateGraph));
      }

      /// <summary>
      /// 
      /// </summary>
      /// <returns></returns>
      public async Task<string> GetUpnById(string objectId)
      {
         try
         {
            User user = await _client.Users[objectId].Request().GetAsync();

            return user.UserPrincipalName;
         }
         catch(ServiceException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
         {
            return null;
         }
      }

      private async Task AuthenticateGraph(HttpRequestMessage request)
      {
         var context = new AuthenticationContext(Authority + _tenantId);

         AuthenticationResult ar = await context.AcquireTokenAsync(
            ResourceUri, new ClientCredential(_clientId, _clientSecret));

         request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", ar.AccessToken);
      }
   }
}
