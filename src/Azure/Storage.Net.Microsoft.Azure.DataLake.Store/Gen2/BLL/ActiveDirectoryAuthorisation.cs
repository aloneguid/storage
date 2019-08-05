using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Gen2.Interfaces;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Gen2.BLL
{
   class ActiveDirectoryAuthorisation : IAuthorisation
   {
      private const string Resource = "https://storage.azure.com/";
      private readonly string _authority;
      private readonly string _clientId;
      private readonly string _clientSecret;

      public ActiveDirectoryAuthorisation(string tenantId, string clientId, string clientSecret)
      {
         _authority = $"https://login.microsoftonline.com/{tenantId}";
         _clientId = clientId;
         _clientSecret = clientSecret;
      }

      public async Task<AuthenticationHeaderValue> AuthoriseAsync(string storageAccountName, string signature)
      {
         var clientCredential = new ClientCredential(_clientId, _clientSecret);
         var authenticationContext = new AuthenticationContext(_authority);
         AuthenticationResult authenticationResult = await authenticationContext.AcquireTokenAsync(Resource, clientCredential);
         return new AuthenticationHeaderValue("Bearer", authenticationResult.AccessToken);
      }
   }
}