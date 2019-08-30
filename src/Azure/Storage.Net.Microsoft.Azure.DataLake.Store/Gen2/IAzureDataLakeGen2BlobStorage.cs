using System.Collections.Generic;
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
      /// Sets permissions on an object
      /// </summary>
      /// <param name="fullPath"></param>
      /// <param name="accessControl">Access control rules. A good idea whould be to retreive them using <see cref="GetAccessControlAsync(string)"/>, modify, and send back via this method.</param>
      /// <returns></returns>
      Task SetAccessControlAsync(string fullPath, AccessControl accessControl);

      /// <summary>
      /// Gets permissions from an object
      /// </summary>
      /// <param name="fullPath"></param>
      /// <returns></returns>
      Task<AccessControl> GetAccessControlAsync(string fullPath);

      /// <summary>
      /// Creates a filesystem
      /// </summary>
      /// <param name="filesystem"></param>
      /// <returns></returns>
      Task CreateFilesystemAsync(string filesystem);

      /// <summary>
      /// Deletes a filesystem
      /// </summary>
      /// <param name="filesystem"></param>
      /// <returns></returns>
      Task DeleteFilesystemAsync(string filesystem);

      /// <summary>
      /// Lists filesystems
      /// </summary>
      /// <returns></returns>
      Task<IEnumerable<string>> ListFilesystemsAsync();

      //Task<string> AclObjectIdToUpnAsync(string objectId);
   }
}
