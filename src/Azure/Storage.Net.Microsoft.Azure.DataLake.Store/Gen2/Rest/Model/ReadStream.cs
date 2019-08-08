using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetBox.Extensions;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest.Model
{
   class ReadStream : Stream
   {
      private long _pos;
      private readonly IDataLakeApi _api;
      private readonly long _length;
      private readonly string _filesystemName;
      private readonly string _path;

      public ReadStream(IDataLakeApi api, long length, string filesystemName, string path)
      {
         _api = api;
         _length = length;
         _filesystemName = filesystemName;
         _path = path;
      }

      #region [ Not important ]

      public override bool CanRead => true;

      public override bool CanSeek => false;

      public override bool CanWrite => false;

      public override long Length => _length;

      public override long Position { get => _pos; set => throw new NotSupportedException(); }

      public override void Flush() { }

      public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
      public override void SetLength(long value) => throw new NotSupportedException();
      public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

      #endregion

      public override int Read(byte[] buffer, int offset, int count)
      {
         return ReadAsync(buffer, offset, count, default).Result;
      }

      private Stream EmptyStream => new MemoryStream(new byte[0]);

      public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
      {
         if(_pos >= _length) return 0;

         if(_pos + count >= _length) count = (int)(_length - _pos - 1);

         string range = $"bytes={_pos}-{count}";

         using(Stream chunk = await _api.ReadPathAsync(_filesystemName, _path, range, EmptyStream).ConfigureAwait(false))
         {
            byte[] dd = chunk.ToByteArray();
            dd.CopyTo(buffer, 0);

            _pos += dd.Length;

            return dd.Length;
         }
      }
   }
}
