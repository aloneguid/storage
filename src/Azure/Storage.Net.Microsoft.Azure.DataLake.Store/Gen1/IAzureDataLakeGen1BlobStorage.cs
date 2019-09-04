using System.Threading.Tasks;
using Storage.Net.Blobs;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen1
{
   /// <summary>
   /// Gen 1 specific API
   /// </summary>
   public interface IAzureDataLakeGen1BlobStorage : IBlobStorage
   {
      /// <summary>
      /// Gets permissions from an object
      /// </summary>
      /// <param name="fullPath"></param>
      /// <returns></returns>
      Task GetAccessControlAsync(string fullPath);
   }
}
