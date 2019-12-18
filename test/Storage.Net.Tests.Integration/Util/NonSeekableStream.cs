using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Storage.Net.Tests.Integration.Util
{
   class NonSeekableStream : Stream
   {
      private readonly Stream parent;

      public NonSeekableStream(Stream parent)
      {
         this.parent = parent;
      }

      public override bool CanRead => parent.CanRead;

      public override bool CanSeek => false;

      public override bool CanWrite => parent.CanWrite;

      public override long Length => parent.Length;

      public override long Position { get => parent.Position; set => parent.Position = value; }

      public override void Flush() => parent.Flush();
      public override int Read(byte[] buffer, int offset, int count) => parent.Read(buffer, offset, count);
      public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
      public override void SetLength(long value) => parent.SetLength(value);
      public override void Write(byte[] buffer, int offset, int count) => parent.Write(buffer, offset, count);
   }
}
