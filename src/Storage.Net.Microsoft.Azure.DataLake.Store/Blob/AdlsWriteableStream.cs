using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Azure.DataLake.Store;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Blob
{
   class AdlsWriteableStream : Stream
   {
      private readonly AdlsOutputStream _parent;

      public AdlsWriteableStream(AdlsOutputStream parent)
      {
         _parent = parent ?? throw new ArgumentNullException(nameof(parent));
      }

      public override bool CanRead => false;

      public override bool CanSeek => false;

      public override bool CanWrite => true;

      public override long Length => _parent.Length;

      public override long Position
      {
         get => _parent.Position;
         set
         {
            _parent.Position = value;
         }
      }

      public override void Flush()
      {
         _parent.Flush();
      }

      public override int Read(byte[] buffer, int offset, int count)
      {
         return _parent.Read(buffer, offset, count);
      }

      public override long Seek(long offset, SeekOrigin origin)
      {
         return _parent.Seek(offset, origin);
      }

      public override void SetLength(long value)
      {
         _parent.SetLength(value);
      }

      public override void Write(byte[] buffer, int offset, int count)
      {
         _parent.Write(buffer, offset, count);
      }

      protected override void Dispose(bool disposing)
      {
         _parent.Dispose();

         base.Dispose(disposing);
      }
   }
}
