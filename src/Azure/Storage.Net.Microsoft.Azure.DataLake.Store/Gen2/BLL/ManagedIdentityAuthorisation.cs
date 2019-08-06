using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.Azure.Services.AppAuthentication;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Gen2.Interfaces;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Gen2.BLL
{
   class ManagedIdentityAuthorisation : IAuthorisation
   {
      private const string Resource = "https://storage.azure.com/";

      public async Task<AuthenticationHeaderValue> AuthoriseAsync(string storageAccountName, string signature)
      {
         var azureServiceTokenProvider = new AzureServiceTokenProvider();
         string token = await azureServiceTokenProvider.GetAccessTokenAsync(Resource);
         return new AuthenticationHeaderValue("Bearer", token);
      }
   }
}
