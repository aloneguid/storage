using Storage.Net.Blob;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Interfaces;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store
{
   public interface IAzureDataLakeGen2Storage : IBlobStorage
   {
      IDataLakeGen2Client Client { get; }
   }
}
