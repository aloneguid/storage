using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Moq;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.BLL;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Interfaces;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Models;
using Xunit;

namespace Storage.Net.Tests.DataLakeGen2
{
   public class DataLakeGen2ClientTests
   {
      private const string FilesystemName = "testfilesystem";
      private const string DirectoryName = "test directory";
      private const string FileName = "test file.txt";

      private const string Acl =
          "group::r-x,user::rwx,user:00000000-0000-0000-0000-000000000000:rwx,default:user:00000000-0000-0000-0000-000000000000:rwx";

      private const string ListResponse = @"
{
  ""paths"": [
    {
      ""etag"": ""Wed, 19 Jun 2019 11:06:27 GMT"",
      ""group"": ""$superuser"",
      ""isDirectory"": ""true"",
      ""lastModified"": ""Wed, 19 Jun 2019 11:06:27 GMT"",
      ""name"": ""directory/directory 2"",
      ""owner"": ""$superuser"",
      ""permissions"": ""rwxr-x---""
    },
    {
      ""contentLength"": ""0"",
      ""etag"": ""Wed, 19 Jun 2019 11:06:39 GMT"",
      ""group"": ""$superuser"",
      ""lastModified"": ""Wed, 19 Jun 2019 11:06:39 GMT"",
      ""name"": ""directory/directory 2/file.txt"",
      ""owner"": ""$superuser"",
      ""permissions"": ""rw-r-----""
    }
  ]
}
";

      private const long ContentLength = 100;
      private readonly MediaTypeHeaderValue _contentType = new MediaTypeHeaderValue("application/octetstream");
      private readonly DateTimeOffset _lastModified = new DateTimeOffset(new DateTime(2019, 6, 21));
      private readonly byte[] _readResponse = { 0, 1, 2, 3 };
      private readonly Mock<IDataLakeGen2RestApi> _restApi;
      private readonly DataLakeGen2Client _sut;

      public DataLakeGen2ClientTests()
      {
         _restApi = new Mock<IDataLakeGen2RestApi>();

         _restApi.Setup(x => x.AppendPathAsync(
                 It.IsAny<string>(), It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<long>()))
             .Returns(Task.FromResult(new HttpResponseMessage()));

         _restApi.Setup(x => x.CreateFilesystemAsync(It.IsAny<string>()))
             .Returns(Task.FromResult(new HttpResponseMessage()));

         _restApi.Setup(x => x.CreateDirectoryAsync(It.IsAny<string>(), It.IsAny<string>()))
             .Returns(Task.FromResult(new HttpResponseMessage()));

         _restApi.Setup(x => x.CreateFileAsync(It.IsAny<string>(), It.IsAny<string>()))
             .Returns(Task.FromResult(new HttpResponseMessage()));

         _restApi.Setup(x => x.DeleteFilesystemAsync(It.IsAny<string>()))
             .Returns(Task.FromResult(new HttpResponseMessage()));

         _restApi.Setup(x => x.DeletePathAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>()))
             .Returns(Task.FromResult(new HttpResponseMessage()));

         _restApi.Setup(x => x.FlushPathAsync(
                 It.IsAny<string>(), It.IsAny<string>(), It.IsAny<long>()))
             .Returns(Task.FromResult(new HttpResponseMessage()));

         _restApi.Setup(x =>
                 x.GetAccessControlAsync(It.IsAny<string>(), It.IsAny<string>()))
             .Returns(Task.FromResult(new HttpResponseMessage
             {
                Headers =
                 {
                        {"x-ms-acl", Acl}, {"x-ms-group", "$superuser"},
                        {"x-ms-owner", "$superuser"}, {"x-ms-permissions", "rwxr-x---"}
                 }
             }));

         _restApi.Setup(x =>
                 x.GetStatusAsync(It.IsAny<string>(), It.IsAny<string>()))
             .Returns(Task.FromResult(new HttpResponseMessage
             {
                Content = new ByteArrayContent(new byte[0])
                {
                   Headers =
                     {
                            ContentLength = ContentLength,
                            ContentType = _contentType,
                            LastModified = _lastModified
                     }
                },
                StatusCode = HttpStatusCode.OK
             }));

         _restApi.Setup(x =>
                 x.ListPathAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<int>()))
             .Returns(Task.FromResult(new HttpResponseMessage { Content = new StringContent(ListResponse) }));

         _restApi.Setup(x =>
                 x.ReadPathAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<long?>(), It.IsAny<long?>()))
             .Returns(Task.FromResult(new HttpResponseMessage { Content = new ByteArrayContent(_readResponse) }));

         _restApi.Setup(x =>
                 x.SetAccessControlAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
             .Returns(Task.FromResult(new HttpResponseMessage()));

         _sut = new DataLakeGen2Client(_restApi.Object);
      }

      [Fact]
      public async Task TestAppendsFile()
      {
         byte[] content = new byte[] { 0, 1, 2 };
         await _sut.AppendFileAsync(FilesystemName, FileName, content, 10);
         _restApi.Verify(x => x.AppendPathAsync(FilesystemName, FileName, content, 10));
      }

      [Fact]
      public async Task TestCreatesDirectory()
      {
         await _sut.CreateDirectoryAsync(FilesystemName, DirectoryName);
         _restApi.Verify(x => x.CreateDirectoryAsync(FilesystemName, DirectoryName));
      }

      [Fact]
      public async Task TestCreatesFile()
      {
         await _sut.CreateFileAsync(FilesystemName, FileName);
         _restApi.Verify(x => x.CreateFileAsync(FilesystemName, FileName));
      }

      [Fact]
      public async Task TestCreatesFilesystem()
      {
         await _sut.CreateFilesystemAsync(FilesystemName);
         _restApi.Verify(x => x.CreateFilesystemAsync(FilesystemName));
      }

      [Fact]
      public async Task TestDeletesDirectory()
      {
         await _sut.DeleteDirectoryAsync(FilesystemName, DirectoryName, true);
         _restApi.Verify(x => x.DeletePathAsync(FilesystemName, DirectoryName, true));
      }

      [Fact]
      public async Task TestDeletesFile()
      {
         await _sut.DeleteFileAsync(FilesystemName, FileName);
         _restApi.Verify(x => x.DeletePathAsync(FilesystemName, FileName, false));
      }

      [Fact]
      public async Task TestDeletesFilesystem()
      {
         await _sut.DeleteFilesystemAsync(FilesystemName);
         _restApi.Verify(x => x.DeleteFilesystemAsync(FilesystemName));
      }

      [Fact]
      public async Task TestGetsAccessControl()
      {
         await _sut.GetAccessControlAsync(FilesystemName, FileName);
         _restApi.Verify(x => x.GetAccessControlAsync(FilesystemName, FileName));
      }

      [Fact]
      public async Task TestGetAccessControlReturnsOwner()
      {
         AccessControl actual = await _sut.GetAccessControlAsync(FilesystemName, FileName);
         Assert.Equal("$superuser", actual.Owner);
      }

      [Fact]
      public async Task TestGetAccessControlReturnsAcl()
      {
         AccessControl actual = await _sut.GetAccessControlAsync(FilesystemName, FileName);

         Assert.Equal("group:", actual.Acl[0].User);
         Assert.True(actual.Acl[0].Access.Read);
         Assert.False(actual.Acl[0].Access.Write);
         Assert.True(actual.Acl[0].Access.Execute);
         Assert.Null(actual.Acl[0].Default);

         Assert.Equal("user:", actual.Acl[1].User);
         Assert.True(actual.Acl[1].Access.Read);
         Assert.True(actual.Acl[1].Access.Write);
         Assert.True(actual.Acl[1].Access.Execute);
         Assert.Null(actual.Acl[1].Default);

         Assert.Equal("user:00000000-0000-0000-0000-000000000000", actual.Acl[2].User);
         Assert.True(actual.Acl[2].Access.Read);
         Assert.True(actual.Acl[2].Access.Write);
         Assert.True(actual.Acl[2].Access.Execute);
         Assert.True(actual.Acl[2].Default.Read);
         Assert.True(actual.Acl[2].Default.Write);
         Assert.True(actual.Acl[2].Default.Execute);
      }

      [Fact]
      public async Task TestGetAccessControlReturnsGroup()
      {
         AccessControl actual = await _sut.GetAccessControlAsync(FilesystemName, FileName);
         Assert.Equal("$superuser", actual.Group);
      }

      [Fact]
      public async Task TestGetAccessControlReturnsPermissions()
      {
         AccessControl actual = await _sut.GetAccessControlAsync(FilesystemName, FileName);
         Assert.Equal("rwxr-x---", actual.Permissions);
      }

      [Fact]
      public async Task TestGetsProperties()
      {
         await _sut.GetPropertiesAsync(FilesystemName, FileName);
         _restApi.Verify(x => x.GetStatusAsync(FilesystemName, FileName));
      }

      [Fact]
      public async Task TestGetPropertiesReturnsContentType()
      {
         Properties actual = await _sut.GetPropertiesAsync(FilesystemName, FileName);
         Assert.Equal(_contentType, actual.ContentType);
      }

      [Fact]
      public async Task TestGetPropertiesReturnsLength()
      {
         Properties actual = await _sut.GetPropertiesAsync(FilesystemName, FileName);
         Assert.Equal(ContentLength, actual.Length);
      }

      [Fact]
      public async Task TestGetPropertiesReturnsLastModified()
      {
         Properties actual = await _sut.GetPropertiesAsync(FilesystemName, FileName);
         Assert.Equal(_lastModified, actual.LastModified);
      }

      [Fact]
      public async Task TestGetPropertiesReturnsExists()
      {
         Properties actual = await _sut.GetPropertiesAsync(FilesystemName, FileName);
         Assert.True(actual.Exists);
      }

      [Fact]
      public async Task TestExistsIsFalseIfNotFound()
      {
         _restApi.Setup(x =>
                 x.GetStatusAsync(It.IsAny<string>(), It.IsAny<string>()))
             .Returns(Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound)));

         Properties actual = await _sut.GetPropertiesAsync(FilesystemName, FileName);
         Assert.False(actual.Exists);
      }

      [Fact]
      public async Task TestFlushesFile()
      {
         await _sut.FlushFileAsync(FilesystemName, FileName, 10);
         _restApi.Verify(x => x.FlushPathAsync(FilesystemName, FileName, 10));
      }

      [Fact]
      public async Task TestListsDirectory()
      {
         await _sut.ListDirectoryAsync(FilesystemName, DirectoryName, true, 2000);
         _restApi.Verify(x => x.ListPathAsync(FilesystemName, DirectoryName, true, 2000));
      }

      [Fact]
      public async Task TestListDirectoryDeserialisesResponse()
      {
         DirectoryList response = await _sut.ListDirectoryAsync(FilesystemName, DirectoryName, true, 2000);
         Assert.Equal(2, response.Paths.Count);
      }

      [Fact]
      public async Task TestListDirectoryDeserialisesEtag()
      {
         DirectoryList response = await _sut.ListDirectoryAsync(FilesystemName, DirectoryName, true, 2000);
         Assert.Equal(new DateTime(2019, 6, 19, 11, 6, 27), response.Paths.First().Etag);
      }

      [Fact]
      public async Task TestListDirectoryDeserialisesGroup()
      {
         DirectoryList response = await _sut.ListDirectoryAsync(FilesystemName, DirectoryName, true, 2000);
         Assert.Equal("$superuser", response.Paths.First().Group);
      }

      [Fact]
      public async Task TestListDirectoryDeserialisesIsDirectory()
      {
         DirectoryList response = await _sut.ListDirectoryAsync(FilesystemName, DirectoryName, true, 2000);
         Assert.True(response.Paths.First().IsDirectory);
      }

      [Fact]
      public async Task TestListDirectoryDeserialisesLastModified()
      {
         DirectoryList response = await _sut.ListDirectoryAsync(FilesystemName, DirectoryName, true, 2000);
         Assert.Equal(new DateTime(2019, 6, 19, 11, 6, 27), response.Paths.First().LastModified);
      }

      [Fact]
      public async Task TestListDirectoryDeserialisesName()
      {
         DirectoryList response = await _sut.ListDirectoryAsync(FilesystemName, DirectoryName, true, 2000);
         Assert.Equal("directory/directory 2", response.Paths.First().Name);
      }

      [Fact]
      public async Task TestListDirectoryDeserialisesOwner()
      {
         DirectoryList response = await _sut.ListDirectoryAsync(FilesystemName, DirectoryName, true, 2000);
         Assert.Equal("$superuser", response.Paths.First().Owner);
      }

      [Fact]
      public async Task TestListDirectoryDeserialisesPermissions()
      {
         DirectoryList response = await _sut.ListDirectoryAsync(FilesystemName, DirectoryName, true, 2000);
         Assert.Equal("rwxr-x---", response.Paths.First().Permissions);
      }

      [Fact]
      public void TestOpenReadReturnsDataLakeGen2Stream()
      {
         Stream actual = _sut.OpenRead(FilesystemName, FileName);
         Assert.IsType<DataLakeGen2Stream>(actual);
      }

      [Fact]
      public async Task TestOpenWriteCreatesFileIfNotExists()
      {
         _restApi.Setup(x =>
                 x.GetStatusAsync(It.IsAny<string>(), It.IsAny<string>()))
             .Returns(Task.FromResult(new HttpResponseMessage
             {
                StatusCode = HttpStatusCode.NotFound
             }));

         await _sut.OpenWriteAsync(FilesystemName, FileName);
         _restApi.Verify(x => x.CreateFileAsync(FilesystemName, FileName));
      }

      [Fact]
      public async Task TestOpenWriteDoesNotCreateFileIfExists()
      {
         await _sut.OpenWriteAsync(FilesystemName, FileName);
         _restApi.Verify(x => x.CreateFileAsync(FilesystemName, FileName), Times.Never);
      }

      [Fact]
      public async Task TestOpenWriteReturnsDataLakeGen2Stream()
      {
         Stream actual = await _sut.OpenWriteAsync(FilesystemName, FileName);
         Assert.IsType<DataLakeGen2Stream>(actual);
      }

      [Fact]
      public async Task TestReadsFile()
      {
         await _sut.ReadFileAsync(FilesystemName, FileName, 10, 15);
         _restApi.Verify(x => x.ReadPathAsync(FilesystemName, FileName, 10, 15));
      }

      [Fact]
      public async Task TestReadFileReturnsResponse()
      {
         byte[] actual = await _sut.ReadFileAsync(FilesystemName, FileName, 0, 100);
         Assert.True(_readResponse.SequenceEqual(actual));
      }

      [Fact]
      public async Task TestSetAccessControl()
      {
         await _sut.SetAccessControlAsync(FilesystemName, FileName, new List<AclItem>
            {
                new AclItem
                {
                    Default = null,
                    Access = new AclPermission
                    {
                        Read = true,
                        Write = true,
                        Execute = true
                    },
                    User = "user:"
                },
                new AclItem
                {
                    Access = new AclPermission
                    {
                        Read = true,
                        Write = false,
                        Execute = true
                    },
                    Default = null,
                    User = "group:"
                },
                new AclItem
                {
                    Access = new AclPermission
                    {
                        Read = true,
                        Write = true,
                        Execute = true
                    },
                    Default = new AclPermission
                    {
                        Read = true,
                        Write = true,
                        Execute = true
                    },
                    User = "user:00000000-0000-0000-0000-000000000000"
                }
            });

         _restApi.Verify(x => x.SetAccessControlAsync(FilesystemName, FileName, Acl));
      }
   }
}