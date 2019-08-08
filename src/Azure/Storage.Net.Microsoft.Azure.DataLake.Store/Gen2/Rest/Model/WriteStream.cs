using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Refit;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest.Model
{
   /// <summary>
   /// This stream calls API on every write operation, that's less than good so can be optimised
   /// </summary>
   class WriteStream : Stream
   {
      private long _pos;
      private readonly IDataLakeApi _api;
      private readonly string _filesystemName;
      private readonly string _relativePath;
      private bool _flushed;

      public WriteStream(IDataLakeApi api, string filesystemName, string relativePath)
      {
         _api = api;
         _filesystemName = filesystemName;
         _relativePath = relativePath;
      }


      public override bool CanRead => false;

      public override bool CanSeek => false;

      public override bool CanWrite => true;

      public override long Length => _pos;

      public override long Position
      {
         get => _pos;
         set
         {
            throw new NotSupportedException();
         }
      }


      public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
      public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
      public override void SetLength(long value) => throw new NotSupportedException();

      public override void Write(byte[] buffer, int offset, int count)
      {
         WriteAsync(buffer, offset, count).Wait();
      }

      public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
      {
         if(_pos == 0)
         {
            try
            {
               await _api.CreatePathAsync(_filesystemName, _relativePath, "file").ConfigureAwait(false);
            }
            catch(ApiException ex) when(ex.StatusCode == HttpStatusCode.NotFound)
            {
               //filesystem doesn't exist, create it
               await _api.CreateFilesystemAsync(_filesystemName).ConfigureAwait(false);

               //now create path again
               await _api.CreatePathAsync(_filesystemName, _relativePath, "file").ConfigureAwait(false);
            }
         }

         await _api.UpdatePathAsync(_filesystemName, _relativePath, "append",
            _pos,
            body: new MemoryStream(buffer, offset, count));

         _pos += count;
      }

      private Stream EmptyStream => new MemoryStream(new byte[0]);

      public override void Flush()
      {
         if(_flushed)
            return;

         _api.UpdatePathAsync(_filesystemName, _relativePath, "flush", _pos, body: EmptyStream).Wait();

         _flushed = true;
      }

      protected override void Dispose(bool disposing)
      {
         Flush();
      }
   }
}
