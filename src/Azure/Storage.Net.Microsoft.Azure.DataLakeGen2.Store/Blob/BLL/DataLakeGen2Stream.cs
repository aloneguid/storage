using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Interfaces;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Models;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.BLL
{
   public class DataLakeGen2Stream : Stream
   {
      private readonly IDataLakeGen2Client _client;
      private readonly string _filesystem;
      private readonly string _path;

      public DataLakeGen2Stream(IDataLakeGen2Client client, string filesystem, string path)
      {
         _client = client;
         _filesystem = filesystem;
         _path = path;
      }

      public override bool CanRead => true;
      public override bool CanSeek => false;
      public override bool CanWrite => true;
      public override long Length => _client.GetPropertiesAsync(_filesystem, _path).Result.Length;
      public override long Position { get; set; }

      public override void Flush()
      {
         throw new NotImplementedException("Flush not implemented. Use FlushAsync.");
      }

      public override Task FlushAsync(CancellationToken cancellationToken)
      {
         return _client.FlushFileAsync(_filesystem, _path, Position, cancellationToken);
      }

      public override int Read(byte[] buffer, int offset, int count)
      {
         throw new NotImplementedException("Read not implemented. Use ReadAsync.");
      }

      public override async Task<int> ReadAsync(byte[] buffer, int offset, int count,
         CancellationToken cancellationToken)
      {
         Properties properties = await _client.GetPropertiesAsync(_filesystem, _path, cancellationToken);

         if(properties.Length == Position)
         {
            return 0;
         }

         long endPosition = Position + count;
         long correctedEndPosition = endPosition < properties.Length ? endPosition : properties.Length;

         byte[] content =
            await _client.ReadFileAsync(_filesystem, _path, Position, correctedEndPosition - 1, cancellationToken);
         content.CopyTo(buffer, offset);
         Position = correctedEndPosition;

         return content.Length;
      }

      public override long Seek(long offset, SeekOrigin origin)
      {
         throw new NotImplementedException();
      }

      public override void SetLength(long value)
      {
         throw new NotImplementedException();
      }

      public override void Write(byte[] buffer, int offset, int count)
      {
         throw new NotImplementedException("Write not implemented. Use WriteAsync.");
      }

      public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
      {
         await _client.AppendFileAsync(_filesystem, _path, buffer.Skip(offset).Take(count).ToArray(), Position,
            cancellationToken);
         Position += count;
         await FlushAsync(cancellationToken);
      }
   }
}