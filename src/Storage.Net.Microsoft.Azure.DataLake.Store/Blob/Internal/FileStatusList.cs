using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.DataLake.Store.RetryPolicies;


namespace Microsoft.Azure.DataLake.Store
{
   /// <summary>
   /// Encapsulates a collection storing the list of directory entries. Once the collection is traversed, retrieves next set of directory entries from server
   /// This is for internal use only. Made public because we want to cast the enumerator to test enumeration with a smaller page size.
   /// </summary>
   internal class FileStatusList : IEnumerator<DirectoryEntry>
   {
      private const int MaxListSize = 200000;

      /// <summary>
      /// Internal collection storing list of directory entries retrieved from server. This is not the whole list of directory entries.
      /// It's size is less than equal to listSize
      /// </summary>
      private List<DirectoryEntry> FileStatus { get; set; }

      /// <summary>
      /// Number of maximum directory entries to retrieve from server at one time
      /// </summary>
      private int _listSize = MaxListSize;
      /// <summary>
      /// Internal property to set the list size
      /// </summary>
      internal int ListSize
      {
         set { _listSize = value > MaxListSize ? MaxListSize : value; }
         private get { return _listSize; }
      }

      /// <summary>
      /// Maximum number of entries to be enumerated as entered by user. If it is -1 then enumerate all the directory entries
      /// </summary>
      private readonly int _maxEntries;
      /// <summary>
      /// Number of entries left to enumerate
      /// </summary>
      private int RemainingEntries { get; set; }
      /// <summary>
      /// Flag indicating enumerate all the directory entries
      /// </summary>
      private bool EnumerateAll { get; }

      /// <summary>
      /// Filename after which we should start the enumeration - entered by user
      /// </summary>
      private readonly string _listAfterClient;
      /// <summary>
      /// Filename after which list of files should be obtained from server next time, updated before everytime the 
      /// </summary>
      private string ListAfterNext { get; set; }

      /// <summary>
      /// Filename till which list of files should be obtained from server
      /// </summary>
      private string ListBefore { get; }
      /// <summary>
      /// ADLS Client
      /// </summary>
      private AdlsClient Client { get; }
      /// <summary>
      /// Way the user or group object will be represented
      /// </summary>
      private UserGroupRepresentation? Ugr { get; }
      /// <summary>
      /// Path of the directory conatianing the sub-directories or files
      /// </summary>
      private string Path { get; }
      /// <summary>
      /// Represents the current directory entry in the internal collection: FileStatus
      /// </summary>
      public DirectoryEntry Current
      {
         get
         {
            try
            {
               return FileStatus[_position];
            }
            catch (IndexOutOfRangeException)
            {
               throw new InvalidOperationException("The index is out of range");
            }
         }
      }
      /// <summary>
      /// Index representating the current position in the internal collection: FileStatus 
      /// </summary>
      private int _position = -1;
      /// <summary>
      /// Immplemented interface property
      /// </summary>
      object IEnumerator.Current => Current;

      /// <summary>
      /// Advances the enumerator to the next element in the internal collection.
      /// If the end of the internal collection is reached, performs a ListStatus call to the server to see if any more directories/files need to be enumerated. If yes
      /// then the internal collection is populated with the next set of directory entries. The internal index pointing to the current element is updated. If not, then returns false.
      /// </summary>
      /// <returns>True if there is a next element to enumerate else false</returns>
      public bool MoveNext()
      {
         //Not called for first time, first time when this is called ListAfterNext will be whatever client has passed
         if (FileStatus != null)
         {
            _position++;
            //FileStatus.Count will be minimum of ListSize and remaining entries asked by user
            if (_position < FileStatus.Count)//Still more data to be enumerated
            {
               if (!EnumerateAll)
               {
                  RemainingEntries--;
               }
               return true;
            }
            //Number of entries wanted by the user is already enumerated
            //RemainingEntries 0 means no need to look at server since last time we retrieved "RemainingEntries" number of entries from server
            if (!EnumerateAll && RemainingEntries <= 0)
            {
               return false;
            }
            //position has reached end of the internal list. But number of directory entries retrieved from last server call is less
            //than list size so no more entries are left on server. So even though RemainingEntries is positive, but there is no data in server only.
            if (_position < ListSize)
            {
               return false;
            }
            //Else we have to look in server to see if we still have any more directory entries to enumerate
            //Obtain the last enumerated entry name so that we can retrieve files after that from server
            ListAfterNext = FileStatus[_position - 1].Name;
         }
         _position = -1;
         OperationResponse resp = new OperationResponse();
         int getListSize = EnumerateAll ? ListSize : Math.Min(ListSize, RemainingEntries);
         FileStatus = Core.ListStatusAsync(Path, ListAfterNext, ListBefore, getListSize, Ugr, Client, new RequestOptions(new ExponentialRetryPolicy()), resp).GetAwaiter().GetResult();
         if (!resp.IsSuccessful)
         {
            throw Client.GetExceptionFromResponse(resp, "Error getting listStatus for path " + Path + " after " + ListAfterNext);
         }
         return MoveNext();
      }

      internal FileStatusList(string listBefore, string listAfter, int maxEntries, UserGroupRepresentation? ugr, AdlsClient client, string path)
      {
         ListBefore = listBefore;
         ListAfterNext = _listAfterClient = listAfter;
         RemainingEntries = _maxEntries = maxEntries;
         if (_maxEntries == -1)
         {
            EnumerateAll = true;
         }
         Ugr = ugr;
         Client = client;
         Path = path;
      }
      /// <summary>
      /// Clears the internal collection and resets the index, ListAfterNext and remaininig entries of collection
      /// </summary>
      public void Reset()
      {
         FileStatus = null;
         _position = -1;
         ListAfterNext = _listAfterClient;
         RemainingEntries = _maxEntries;
      }
      /// <summary>
      /// Disposes the enumerable
      /// </summary>
      public void Dispose()
      {
         FileStatus = null;
      }
   }
}