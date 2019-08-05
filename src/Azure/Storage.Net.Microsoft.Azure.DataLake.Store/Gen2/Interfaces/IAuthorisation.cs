using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Gen2.Interfaces
{
   interface IAuthorisation
   {
      Task<AuthenticationHeaderValue> AuthoriseAsync(string storageAccountName, string signature);
   }
}