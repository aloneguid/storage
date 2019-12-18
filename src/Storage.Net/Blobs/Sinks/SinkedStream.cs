using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IO;

namespace Storage.Net.Blobs.Sinks
{
   class SinkedStream : Stream
   {
      private readonly Stream _parentReadStream;
      private readonly string _fullBlobPath;
      private readonly ITransformSink[] _transformSinks;
      private static readonly RecyclableMemoryStreamManager _streamManager = new RecyclableMemoryStreamManager();
      private readonly MemoryStream _ms;

      public SinkedStream(Stream parentReadStream, string fullBlobPath, params ITransformSink[] transformSinks)
      {
         _parentReadStream = parentReadStream ?? throw new ArgumentNullException(nameof(parentReadStream));
         _fullBlobPath = fullBlobPath ?? throw new ArgumentNullException(nameof(fullBlobPath));
         _transformSinks = transformSinks ?? throw new ArgumentNullException(nameof(transformSinks));
         if(!_parentReadStream.CanRead)
            throw new ArgumentException("stream is not readable", nameof(parentReadStream));

         _ms = TransformToMemoryStream(fullBlobPath, parentReadStream, transformSinks);
      }

      public override bool CanRead => true;

      public override bool CanSeek => false;

      public override bool CanWrite => false;

      public override long Length => _ms.Length;

      public override long Position { get => _ms.Position; set => throw new NotSupportedException(); }

      public override void Flush() => _ms.Flush();

      public override int Read(byte[] buffer, int offset, int count) =>
         _ms.Read(buffer, offset, count);

      public override long Seek(long offset, SeekOrigin origin)
      {
         return _ms.Seek(offset, origin);
      }

      public override void SetLength(long value) => throw new NotSupportedException();
      public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

      private static MemoryStream TransformToMemoryStream(string fullPath, Stream parentReadStream, ITransformSink[] sinks)
      {
         MemoryStream ms = _streamManager.GetStream();

         try
         {
            //layer sinks and move data to memory stream

            Stream dest = ms;
            var chain = new List<Stream>(sinks.Length);
            foreach(ITransformSink sink in sinks)
            {
               dest = sink.OpenWriteStream(fullPath, dest);
               chain.Add(dest);
            }

            parentReadStream.CopyTo(dest);
            
            //flush all streams, especially cryptostreams

            foreach(Stream chainStream in chain)
            {
               chainStream.Flush();
               if(chainStream is CryptoStream cs)
               {
                  cs.FlushFinalBlock();
               }
            }

            //rewind pointer to the beginning
            ms.Seek(0, SeekOrigin.Begin);

            return ms;
         }
         catch
         {
            ms.Dispose();  //reclaim stream on any internal error
            throw;
         }
      }

      protected override void Dispose(bool disposing)
      {
         if(_ms != null)
         {
            _ms.Dispose();
         }
      }
   }
}
