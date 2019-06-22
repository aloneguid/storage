using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Interfaces
{
   public interface IHttpClientWrapper
   {
      Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken);
   }
}