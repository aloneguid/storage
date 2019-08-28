using System;
using System.Collections.Generic;
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
      }

      /// <summary>
      /// 
      /// </summary>
      /// <returns></returns>
      public async Task DoAsync()
      {
         var client = new GraphServiceClient(new DelegateAuthenticationProvider(AuthenticateGraph));

         User r = await client.Me.Request().GetAsync();
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
