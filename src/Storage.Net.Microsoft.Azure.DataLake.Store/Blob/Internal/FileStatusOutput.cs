using System.Collections;
using System.Collections.Generic;


namespace Microsoft.Azure.DataLake.Store
{
   /// <summary>
   /// Enumerable that exposes enumerator:FileStatusList
   /// </summary>
   internal class FileStatusOutput : IEnumerable<DirectoryEntry>
   {
      /// <summary>
      /// Number of maximum directory entries to be retrieved from server. If -1 then retrieve all entries
      /// </summary>
      private readonly int _maxEntries;

      /// <summary>
      /// Filename after which list of files should be obtained from server
      /// </summary>
      private readonly string _listAfter;

      /// <summary>
      /// Filename till which list of files should be obtained from server
      /// </summary>
      private readonly string _listBefore;

      /// <summary>
      /// ADLS Client
      /// </summary>
      private readonly AdlsClient _client;

      /// <summary>
      /// Way the user or group object will be represented
      /// </summary>
      private readonly UserGroupRepresentation? _ugr;

      /// <summary>
      /// Path of the directory containing the sub-directories or files
      /// </summary>
      private readonly string _path;
      /// <summary>
      /// Returns the enumerator
      /// </summary>
      /// <returns></returns>
      public IEnumerator<DirectoryEntry> GetEnumerator()
      {
         return new FileStatusList(_listBefore, _listAfter, _maxEntries, _ugr, _client, _path);
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return GetEnumerator();
      }

      internal FileStatusOutput(string listBefore, string listAfter, int maxEntries, UserGroupRepresentation? ugr, AdlsClient client, string path)
      {
         _listBefore = listBefore;
         _maxEntries = maxEntries;
         _listAfter = listAfter;
         _ugr = ugr;
         _client = client;
         _path = path;
      }
   }
}