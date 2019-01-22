using System;
using System.IO;

namespace Storage.Net.Streaming
{
   /// <summary>
   /// Fixes deficiencies in thrird party streaming impelmentations
   /// </summary>
   public class FixedStream : Stream
   {
      private readonly Stream _parent;
      private readonly long? _length;
      private readonly Action<FixedStream> _disposeCallback;
      private long _position;

      /// <summary>
      /// Creates a new instance
      /// </summary>
      public FixedStream(Stream parent,
         long? length = null,
         Action<FixedStream> disposeCallback = null)
      {
         _parent = parent ?? throw new ArgumentNullException(nameof(parent));
         _length = length;
         _disposeCallback = disposeCallback;
      }

      /// <summary>
      /// Gets original parent stream
      /// </summary>
      public Stream Parent => _parent;

      /// <summary>
      /// Gets or sets an optional tag
      /// </summary>
      public object Tag { get; set; }

      /// <summary>
      /// Gets parent's <see cref="CanRead"/>
      /// </summary>
      public override bool CanRead => _parent.CanRead;

      /// <summary>
      /// Gets parent's <see cref="CanSeek"/>
      /// </summary>
      public override bool CanSeek => _parent.CanSeek;

      /// <summary>
      /// Gets parent's <see cref="CanWrite"/>
      /// </summary>
      public override bool CanWrite => _parent.CanWrite;

      /// <summary>
      /// Gets stream leanth by returning either length passed in the constructor, or parent's length, in that order.
      /// </summary>
      public override long Length => _length == null ? _parent.Length : _length.Value;

      /// <summary>
      /// Gets or sets current potision. This counter is maintained internally and parent's position is not used.
      /// </summary>
      public override long Position
      {
         get => _parent.Position;
         set
         {
            _parent.Position = value;
         }
      }

      /// <summary>
      /// Flushes the parent
      /// </summary>
      public override void Flush()
      {
         _parent.Flush();
      }

      /// <summary>
      /// Reads from parent and updates internal position counter
      /// </summary>
      public override int Read(byte[] buffer, int offset, int count)
      {
         int read = _parent.Read(buffer, offset, count);
         _position += read;
         return read;
      }

      /// <summary>
      /// Seeks on parent
      /// </summary>
      public override long Seek(long offset, SeekOrigin origin)
      {
         return (_position = _parent.Seek(offset, origin));
      }

      /// <summary>
      /// Sets length on parent
      /// </summary>
      public override void SetLength(long value)
      {
         _parent.SetLength(value);
      }

      /// <summary>
      /// Writes to parent and updates the internal position.
      /// </summary>
      public override void Write(byte[] buffer, int offset, int count)
      {
         _parent.Write(buffer, offset, count);

         _position += count;
      }

      /// <summary>
      /// Calls back dispose if passed in the constructor, and calls parent's dispose
      /// </summary>
      /// <param name="disposing"></param>
      protected override void Dispose(bool disposing)
      {
         _disposeCallback?.Invoke(this);

         base.Dispose(disposing);
      }
   }
}
