using System;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Gen2.Interfaces;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Gen2.BLL
{
   class SharedAccessKeyAuthorisation : IAuthorisation
   {
      private readonly string _sharedAccessKey;

      public SharedAccessKeyAuthorisation(string sharedAccessKey)
      {
         _sharedAccessKey = sharedAccessKey;
      }

      public Task<AuthenticationHeaderValue> AuthoriseAsync(string storageAccountName, string signature)
      {
         using(var sha256 = new HMACSHA256(Convert.FromBase64String(_sharedAccessKey)))
         {
            string hashed = Convert.ToBase64String(sha256.ComputeHash(Encoding.UTF8.GetBytes(signature)));
            var authenticationHeader =
                new AuthenticationHeaderValue("SharedKey", storageAccountName + ":" + hashed);

            return Task.FromResult(authenticationHeader);
         }
      }
   }
}