using System;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.BLL;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Interfaces;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Models;
using Xunit;

namespace Storage.Net.Tests.DataLakeGen2
{
   public class DataLakeGen2StreamTests
   {
      private const string Filesystem = "test filesystem";
      private const string FilePath = "test directory/test file.txt";

      private readonly Properties _properties = new Properties
      {
         ContentType = new MediaTypeHeaderValue("application/octetstream"),
         LastModified = new DateTimeOffset(new DateTime(2019, 6, 21)),
         Length = 1000
      };

      private readonly Mock<IDataLakeGen2Client> _client;
      private readonly byte[] _data;
      private readonly DataLakeGen2Stream _sut;

      public DataLakeGen2StreamTests()
      {
         _data = Enumerable.Range(0, (int)_properties.Length).Select(x => (byte)x).ToArray();

         _client = new Mock<IDataLakeGen2Client>();

         _client.Setup(x => x.GetPropertiesAsync(It.IsAny<string>(), It.IsAny<string>(), CancellationToken.None))
            .Returns(Task.FromResult(_properties));

         long? start = -1;
         long? end = -1;
         _client.Setup(x =>
               x.ReadFileAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<long?>(), It.IsAny<long?>(),
                  CancellationToken.None))
            .Callback<string, string, long?, long?, CancellationToken>((a, b, c, d, e) =>
            {
               start = c;
               end = d;
            })
            .Returns(() => Task.FromResult(_data.Skip((int)start.GetValueOrDefault())
               .Take((int)end.GetValueOrDefault() - (int)start.GetValueOrDefault() + 1).ToArray()));

         _sut = new DataLakeGen2Stream(_client.Object, Filesystem, FilePath);
      }

      [Fact]
      public void TestCanReadReturnsTrue()
      {
         Assert.True(_sut.CanRead);
      }

      [Fact]
      public void TestCanSeekReturnsFalse()
      {
         Assert.False(_sut.CanSeek);
      }

      [Fact]
      public void TestCanSeekReturnsTrue()
      {
         Assert.True(_sut.CanWrite);
      }

      [Fact]
      public async Task TestReadGetsProperties()
      {
         await _sut.ReadAsync(new byte[0], CancellationToken.None);

         _client.Verify(x => x.GetPropertiesAsync(Filesystem, FilePath, CancellationToken.None));
      }

      [Fact]
      public async Task TestReadGetsPropertiesForEachCall()
      {
         await _sut.ReadAsync(new byte[0], CancellationToken.None);
         await _sut.ReadAsync(new byte[0], CancellationToken.None);

         _client.Verify(x => x.GetPropertiesAsync(Filesystem, FilePath, CancellationToken.None), Times.Exactly(2));
      }

      [Fact]
      public async Task TestReadsFromZeroPosition()
      {
         const int length = 100;
         await _sut.ReadAsync(new byte[length], CancellationToken.None);

         _client.Verify(x => x.ReadFileAsync(Filesystem, FilePath, 0, length - 1, CancellationToken.None));
      }

      [Fact]
      public async Task TestReadsFromNextPosition()
      {
         const int length = 100;
         await _sut.ReadAsync(new byte[length], CancellationToken.None);
         await _sut.ReadAsync(new byte[length], CancellationToken.None);

         _client.Verify(x => x.ReadFileAsync(Filesystem, FilePath, length, length * 2 - 1, CancellationToken.None));
      }

      [Fact]
      public async Task TestCopiesContentToArray()
      {
         const int length = 100;
         byte[] content = new byte[length];

         await _sut.ReadAsync(content, CancellationToken.None);
         Assert.True(_data.Take(length).SequenceEqual(content));
      }

      [Fact]
      public async Task TestFlushes()
      {
         await _sut.FlushAsync(CancellationToken.None);
         _client.Verify(x => x.FlushFileAsync(Filesystem, FilePath, 0, CancellationToken.None));
      }

      [Fact]
      public async Task TestWriteFlushes()
      {
         const int count = 3;
         await _sut.WriteAsync(new byte[10], 0, 3);
         _client.Verify(x => x.FlushFileAsync(Filesystem, FilePath, count, CancellationToken.None));
      }

      [Fact]
      public async Task TestWrites()
      {
         byte[] content = {0, 1, 2};
         await _sut.WriteAsync(content, 0, content.Length);

         _client.Verify(x =>
            x.AppendFileAsync(Filesystem, FilePath, It.Is<byte[]>(y => content.SequenceEqual(y)), 0,
               CancellationToken.None));
      }

      [Fact]
      public async Task TestWritesWithCurrentPosition()
      {
         byte[] content1 = {0, 1, 2};
         byte[] content2 = {3, 4, 5};
         await _sut.WriteAsync(content1, 0, content1.Length);
         await _sut.WriteAsync(content2, 0, content2.Length);

         _client.Verify(x =>
            x.AppendFileAsync(Filesystem, FilePath, It.Is<byte[]>(y => content2.SequenceEqual(y)), 3,
               CancellationToken.None));
      }

      [Fact]
      public async Task TestPositionStoresCurrentPosition()
      {
         byte[] content = {0, 1, 2};
         await _sut.WriteAsync(content, 0, content.Length);

         Assert.Equal(content.Length, _sut.Position);
      }

      [Fact]
      public void TestPositionGetLength()
      {
         Assert.Equal(_properties.Length, _sut.Length);
      }
   }
}