using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Model;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2
{
   /// <summary>
   /// Extended interface for ADLS Gen2 functionality
   /// </summary>
   public interface IAzureDataLakeGen2BlobStorage : IBlobStorage
   {
      /// <summary>
      /// 
      /// </summary>
      /// <param name="fullPath"></param>
      /// <returns></returns>
      Task SetAclAsync(string fullPath);

      /// <summary>
      /// 
      /// </summary>
      /// <param name="fullPath"></param>
      /// <returns></returns>
      Task<AccessControl> GetAccessControlAsync(string fullPath);
   }
}
