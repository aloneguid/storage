using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Refit;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest.Model;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest
{
   /// <summary>
   /// Refit interface wrapping the calls.
   /// REST API documentation - https://docs.microsoft.com/en-us/rest/api/storageservices/data-lake-storage-gen2
   /// </summary>
   interface IDataLakeApi
   {
      /// <summary>
      /// https://docs.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/filesystem/list
      /// </summary>
      /// <returns></returns>
      [Get("/?resource=account")]
      Task<FilesystemList> ListFilesystemsAsync(
         string prefix = null,
         string continuation = null,
         int? maxResults = null,
         int? timeoutSeconds = null,
         [AliasAs("x-ms-client-request-id")] string xMsClientRequestId = null);
   }
}
