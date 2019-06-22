using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.BLL;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Interfaces;
using Xunit;

namespace Storage.Net.Tests.DataLakeGen2
{
   public class DataLakeGen2RestApiTests
   {
      private const string StorageAccountName = "teststorage";
      private const string FilesystemName = "testfilesystem";
      private const string DirectoryName = "test directory";
      private const string FileName = "test file.txt";
      private readonly AuthenticationHeaderValue _authenticationHeaderValue;
      private readonly Mock<IAuthorisation> _authorisation;
      private readonly Mock<IDateTimeWrapper> _dateTime;
      private readonly DateTime _dateTimeReference;
      private readonly Mock<IHttpClientWrapper> _httpClient;
      private readonly HttpResponseMessage _responseReference;
      private readonly DataLakeGen2RestApi _sut;

      public DataLakeGen2RestApiTests()
      {
         _responseReference = new HttpResponseMessage(HttpStatusCode.OK);

         _httpClient = new Mock<IHttpClientWrapper>();
         _httpClient.Setup(x => x.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(_responseReference));

         _authenticationHeaderValue = new AuthenticationHeaderValue("Bearer", "Token");

         _authorisation = new Mock<IAuthorisation>();
         _authorisation.Setup(x => x.AuthoriseAsync(It.IsAny<string>(), It.IsAny<string>()))
            .Returns(Task.FromResult(_authenticationHeaderValue));

         _dateTimeReference = new DateTime(2019, 6, 17);

         _dateTime = new Mock<IDateTimeWrapper>();
         _dateTime.Setup(x => x.Now).Returns(_dateTimeReference);

         _sut = new DataLakeGen2RestApi(_httpClient.Object, _authorisation.Object, _dateTime.Object,
            StorageAccountName);
      }

      [Fact]
      public async Task TestAppendPathRequestsWithHttpVerb()
      {
         HttpMethod expected = HttpMethod.Patch;
         await _sut.AppendPathAsync(FilesystemName, FileName, new byte[] {0, 1, 2}, 0);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.Method == expected
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestAppendPathRequestsWithUri()
      {
         var expected =
            new Uri(
               $"https://{StorageAccountName}.dfs.core.windows.net/{FilesystemName}/{FileName}?action=append&position=0&timeout=60");
         await _sut.AppendPathAsync(FilesystemName, FileName, new byte[] {0, 1, 2}, 0);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.RequestUri == expected
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestAppendPathRequestsWithSignature()
      {
         byte[] data = {0, 1, 2};
         string expected =
            $"PATCH\n\n\n{data.Length}\n\n\n\n\n\n\n\n\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}/{Uri.EscapeDataString(FileName)}\naction:append\nposition:0\ntimeout:60";
         await _sut.AppendPathAsync(FilesystemName, FileName, data, 0);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestAppendPathRequestsWithAuthorisationHeader()
      {
         await _sut.AppendPathAsync(FilesystemName, FileName, new byte[] {0, 1, 2}, 0);

         _httpClient.Verify(x =>
            x.SendAsync(
               It.Is<HttpRequestMessage>(y => y.Headers.Authorization.Equals(_authenticationHeaderValue)
               ), CancellationToken.None));
      }

      [Fact]
      public async Task TestAppendPathRequestsWithMsDateHeader()
      {
         await _sut.AppendPathAsync(FilesystemName, FileName, new byte[] {0, 1, 2}, 0);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-date").Value.First() == _dateTimeReference.ToString("R")
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestAppendPathRequestsWithMsVersionHeader()
      {
         await _sut.AppendPathAsync(FilesystemName, FileName, new byte[] {0, 1, 2}, 0);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-version").Value.First() == "2018-11-09"
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestAppendPathRequestsWithContent()
      {
         byte[] content = {0, 1, 2};
         await _sut.AppendPathAsync(FilesystemName, FileName, content, 0);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Content.ReadAsByteArrayAsync().Result.SequenceEqual(content)
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestAppendPathReturnsResponse()
      {
         HttpResponseMessage actual = await _sut.AppendPathAsync(FilesystemName, FileName, new byte[] {0, 1, 2}, 0);

         Assert.Equal(_responseReference, actual);
      }

      [Fact]
      public async Task TestCreateDirectoryRequestsWithHttpVerb()
      {
         HttpMethod expected = HttpMethod.Put;
         await _sut.CreateDirectoryAsync(FilesystemName, DirectoryName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.Method == expected
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateDirectoryRequestsWithUri()
      {
         var expected =
            new Uri(
               $"https://{StorageAccountName}.dfs.core.windows.net/{FilesystemName}/{DirectoryName}?resource=directory&timeout=60");
         await _sut.CreateDirectoryAsync(FilesystemName, DirectoryName);

         _httpClient.Verify(x => x.SendAsync(
            It.Is<HttpRequestMessage>(y => y.RequestUri == expected
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateDirectoryRequestsWithSignature()
      {
         string expected =
            $"PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}/{Uri.EscapeDataString(DirectoryName)}\nresource:directory\ntimeout:60";
         await _sut.CreateDirectoryAsync(FilesystemName, DirectoryName);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestCreateDirectoryRequestsWithAuthorisationHeader()
      {
         await _sut.CreateDirectoryAsync(FilesystemName, DirectoryName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.Authorization.Equals(_authenticationHeaderValue)
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateDirectoryRequestsWithMsDateHeader()
      {
         await _sut.CreateDirectoryAsync(FilesystemName, DirectoryName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-date").Value.First() == _dateTimeReference.ToString("R")
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateDirectoryRequestsWithMsVersionHeader()
      {
         await _sut.CreateDirectoryAsync(FilesystemName, DirectoryName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-version").Value.First() == "2018-11-09"
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateDirectoryRequestsWithEmptyContent()
      {
         await _sut.CreateDirectoryAsync(FilesystemName, DirectoryName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Content.Headers.ContentLength == 0
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateDirectoryReturnsResponse()
      {
         HttpResponseMessage actual = await _sut.CreateDirectoryAsync(FilesystemName, DirectoryName);

         Assert.Equal(_responseReference, actual);
      }

      [Fact]
      public async Task TestCreateFileRequestsWithHttpVerb()
      {
         HttpMethod expected = HttpMethod.Put;
         await _sut.CreateFileAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.Method == expected
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateFileRequestsWithUri()
      {
         var expected =
            new Uri(
               $"https://{StorageAccountName}.dfs.core.windows.net/{FilesystemName}/{FileName}?resource=file&timeout=60");
         await _sut.CreateFileAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.RequestUri == expected), CancellationToken.None));
      }


      [Fact]
      public async Task TestCreateFileRequestsWithSignature()
      {
         string expected =
            $"PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}/{Uri.EscapeDataString(FileName)}\nresource:file\ntimeout:60";
         await _sut.CreateFileAsync(FilesystemName, FileName);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestCreateFileRequestsWithAuthorisationHeader()
      {
         await _sut.CreateFileAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.Authorization.Equals(_authenticationHeaderValue)
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateFileRequestsWithMsDateHeader()
      {
         await _sut.CreateFileAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-date").Value.First() == _dateTimeReference.ToString("R")
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateFileRequestsWithMsVersionHeader()
      {
         await _sut.CreateFileAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-version").Value.First() == "2018-11-09"
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateFileRequestsWithEmptyContent()
      {
         await _sut.CreateFileAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Content.Headers.ContentLength == 0
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateFileReturnsResponse()
      {
         HttpResponseMessage actual = await _sut.CreateFileAsync(FilesystemName, FileName);

         Assert.Equal(_responseReference, actual);
      }

      [Fact]
      public async Task TestCreateFilesystemRequestsWithHttpVerb()
      {
         HttpMethod expected = HttpMethod.Put;
         await _sut.CreateFilesystemAsync(FilesystemName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.Method == expected
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateFilesystemRequestsWithUri()
      {
         var expected =
            new Uri(
               $"https://{StorageAccountName}.dfs.core.windows.net/{FilesystemName}?resource=filesystem&timeout=60");
         await _sut.CreateFilesystemAsync(FilesystemName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.RequestUri == expected
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateFilesystemRequestsWithSignature()
      {
         string expected =
            $"PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}\nresource:filesystem\ntimeout:60";
         await _sut.CreateFilesystemAsync(FilesystemName);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestCreateFilesystemRequestsWithAuthorisationHeader()
      {
         await _sut.CreateFilesystemAsync(FilesystemName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.Authorization.Equals(_authenticationHeaderValue)
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateFilesystemRequestsWithMsDateHeader()
      {
         await _sut.CreateFilesystemAsync(FilesystemName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-date").Value.First() == _dateTimeReference.ToString("R")
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateFilesystemRequestsWithMsVersionHeader()
      {
         await _sut.CreateFilesystemAsync(FilesystemName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-version").Value.First() == "2018-11-09"
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateFilesystemRequestsWithEmptyContent()
      {
         await _sut.CreateFilesystemAsync(FilesystemName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Content.Headers.ContentLength == 0
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestCreateFilesystemReturnsResponse()
      {
         HttpResponseMessage actual = await _sut.CreateFilesystemAsync(FilesystemName);

         Assert.Equal(_responseReference, actual);
      }

      [Fact]
      public async Task TestDeleteFilesystemRequestsWithHttpVerb()
      {
         HttpMethod expected = HttpMethod.Delete;
         await _sut.DeleteFilesystemAsync(FilesystemName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.Method == expected
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestDeleteFilesystemRequestsWithUri()
      {
         var expected =
            new Uri(
               $"https://{StorageAccountName}.dfs.core.windows.net/{FilesystemName}?resource=filesystem&timeout=60");
         await _sut.DeleteFilesystemAsync(FilesystemName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.RequestUri == expected
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestDeleteFilesystemRequestsWithSignature()
      {
         string expected =
            $"DELETE\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}\nresource:filesystem\ntimeout:60";
         await _sut.DeleteFilesystemAsync(FilesystemName);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestDeleteFilesystemRequestsWithAuthorisationHeader()
      {
         await _sut.DeleteFilesystemAsync(FilesystemName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.Authorization.Equals(_authenticationHeaderValue)
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestDeleteFilesystemRequestsWithMsDateHeader()
      {
         await _sut.DeleteFilesystemAsync(FilesystemName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-date").Value.First() == _dateTimeReference.ToString("R")
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestDeleteFilesystemRequestsWithMsVersionHeader()
      {
         await _sut.DeleteFilesystemAsync(FilesystemName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-version").Value.First() == "2018-11-09"
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestDeleteFilesystemRequestsWithEmptyContent()
      {
         await _sut.DeleteFilesystemAsync(FilesystemName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Content.Headers.ContentLength == 0
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestDeleteFilesystemReturnsResponse()
      {
         HttpResponseMessage actual = await _sut.DeleteFilesystemAsync(FilesystemName);

         Assert.Equal(_responseReference, actual);
      }

      [Fact]
      public async Task TestDeletePathRequestsWithHttpVerb()
      {
         HttpMethod expected = HttpMethod.Delete;
         await _sut.DeletePathAsync(FilesystemName, DirectoryName, true);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.Method == expected),
               CancellationToken.None));
      }

      [Fact]
      public async Task TestDeletePathRequestsWithUriForRecursive()
      {
         var expected =
            new Uri(
               $"https://{StorageAccountName}.dfs.core.windows.net/{FilesystemName}/{DirectoryName}?recursive=true&timeout=60");
         await _sut.DeletePathAsync(FilesystemName, DirectoryName, true);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.RequestUri == expected), CancellationToken.None));
      }

      [Fact]
      public async Task TestDeletePathRequestsWithUriForNonRecursive()
      {
         var expected =
            new Uri(
               $"https://{StorageAccountName}.dfs.core.windows.net/{FilesystemName}/{DirectoryName}?recursive=false&timeout=60");
         await _sut.DeletePathAsync(FilesystemName, DirectoryName, false);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.RequestUri == expected), CancellationToken.None));
      }

      [Fact]
      public async Task TestDeletePathRequestsWithSignatureForRecursive()
      {
         string expected =
            $"DELETE\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}/{Uri.EscapeDataString(DirectoryName)}\nrecursive:true\ntimeout:60";
         await _sut.DeletePathAsync(FilesystemName, DirectoryName, true);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestDeletePathRequestsWithSignatureForNonRecursive()
      {
         string expected =
            $"DELETE\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}/{Uri.EscapeDataString(DirectoryName)}\nrecursive:false\ntimeout:60";
         await _sut.DeletePathAsync(FilesystemName, DirectoryName, false);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestDeletePathRequestsWithAuthorisationHeader()
      {
         await _sut.DeletePathAsync(FilesystemName, DirectoryName, true);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.Authorization.Equals(_authenticationHeaderValue)
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestDeletePathRequestsWithMsDateHeader()
      {
         await _sut.DeletePathAsync(FilesystemName, DirectoryName, true);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-date").Value.First() == _dateTimeReference.ToString("R")
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestDeletePathRequestsWithMsVersionHeader()
      {
         await _sut.DeletePathAsync(FilesystemName, DirectoryName, true);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-version").Value.First() == "2018-11-09"
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestDeletePathRequestsWithEmptyContent()
      {
         await _sut.DeletePathAsync(FilesystemName, DirectoryName, true);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Content.Headers.ContentLength == 0
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestDeletePathReturnsResponse()
      {
         HttpResponseMessage actual = await _sut.DeletePathAsync(FilesystemName, DirectoryName, true);

         Assert.Equal(_responseReference, actual);
      }

      [Fact]
      public async Task TestGetAccessControlRequestsWithHttpVerb()
      {
         HttpMethod expected = HttpMethod.Head;
         await _sut.GetAccessControlAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.Method == expected), CancellationToken.None));
      }


      [Fact]
      public async Task TestGetAccessControlRequestsWithUri()
      {
         var expected =
            new Uri(
               $"https://{StorageAccountName}.dfs.core.windows.net/{FilesystemName}/{FileName}?action=getaccesscontrol&upn=true&timeout=60");
         await _sut.GetAccessControlAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.RequestUri == expected), CancellationToken.None));
      }

      [Fact]
      public async Task TestGetAccessControlRequestsWithSignature()
      {
         string expected =
            $"HEAD\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}/{Uri.EscapeDataString(FileName)}\naction:getaccesscontrol\ntimeout:60\nupn:true";
         await _sut.GetAccessControlAsync(FilesystemName, FileName);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestGetAccessControlRequestsWithAuthorisationHeader()
      {
         await _sut.GetAccessControlAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.Authorization.Equals(_authenticationHeaderValue)
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestGetAccessControlRequestsWithMsDateHeader()
      {
         await _sut.GetAccessControlAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-date").Value.First() == _dateTimeReference.ToString("R")
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestGetAccessControlRequestsWithMsVersionHeader()
      {
         await _sut.GetAccessControlAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-version").Value.First() == "2018-11-09"
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestGetAccessControlRequestsWithEmptyContent()
      {
         await _sut.GetAccessControlAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Content.Headers.ContentLength == 0
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestGetAccessControlReturnsResponse()
      {
         HttpResponseMessage actual = await _sut.GetAccessControlAsync(FilesystemName, FileName);

         Assert.Equal(_responseReference, actual);
      }

      [Fact]
      public async Task TestGetStatusRequestsWithHttpVerb()
      {
         HttpMethod expected = HttpMethod.Head;
         await _sut.GetStatusAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.Method == expected), CancellationToken.None));
      }


      [Fact]
      public async Task TestGetStatusRequestsWithUri()
      {
         var expected =
            new Uri(
               $"https://{StorageAccountName}.dfs.core.windows.net/{FilesystemName}/{FileName}?timeout=60");
         await _sut.GetStatusAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.RequestUri == expected), CancellationToken.None));
      }

      [Fact]
      public async Task TestGetStatusRequestsWithSignature()
      {
         string expected =
            $"HEAD\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}/{Uri.EscapeDataString(FileName)}\ntimeout:60";
         await _sut.GetStatusAsync(FilesystemName, FileName);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestGetStatusRequestsWithAuthorisationHeader()
      {
         await _sut.GetStatusAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.Authorization.Equals(_authenticationHeaderValue)
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestGetStatusRequestsWithMsDateHeader()
      {
         await _sut.GetStatusAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-date").Value.First() == _dateTimeReference.ToString("R")
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestGetStatusRequestsWithMsVersionHeader()
      {
         await _sut.GetStatusAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-version").Value.First() == "2018-11-09"
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestGetStatusRequestsWithEmptyContent()
      {
         await _sut.GetStatusAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Content.Headers.ContentLength == 0
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestGetStatusReturnsResponse()
      {
         HttpResponseMessage actual = await _sut.GetStatusAsync(FilesystemName, FileName);

         Assert.Equal(_responseReference, actual);
      }

      [Fact]
      public async Task TestFlushPathRequestsWithHttpVerb()
      {
         HttpMethod expected = HttpMethod.Patch;
         await _sut.FlushPathAsync(FilesystemName, FileName, 3);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.Method == expected), CancellationToken.None));
      }

      [Fact]
      public async Task TestFlushPathRequestsWithUri()
      {
         var expected =
            new Uri(
               $"https://{StorageAccountName}.dfs.core.windows.net/{FilesystemName}/{FileName}?action=flush&position=3&timeout=60");
         await _sut.FlushPathAsync(FilesystemName, FileName, 3);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.RequestUri == expected), CancellationToken.None));
      }

      [Fact]
      public async Task TestFlushPathRequestsWithSignature()
      {
         string expected =
            $"PATCH\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}/{Uri.EscapeDataString(FileName)}\naction:flush\nposition:3\ntimeout:60";
         await _sut.FlushPathAsync(FilesystemName, FileName, 3);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestFlushPathRequestsWithAuthorisationHeader()
      {
         await _sut.FlushPathAsync(FilesystemName, FileName, 0);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.Authorization.Equals(_authenticationHeaderValue)
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestFlushPathRequestsWithMsDateHeader()
      {
         await _sut.FlushPathAsync(FilesystemName, FileName, 0);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-date").Value.First() == _dateTimeReference.ToString("R")
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestFlushPathRequestsWithMsVersionHeader()
      {
         await _sut.FlushPathAsync(FilesystemName, FileName, 0);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-version").Value.First() == "2018-11-09"
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestFlushPathRequestsWithEmptyContent()
      {
         await _sut.FlushPathAsync(FilesystemName, FileName, 0);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Content.Headers.ContentLength == 0
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestFlushPathReturnsResponse()
      {
         HttpResponseMessage actual = await _sut.FlushPathAsync(FilesystemName, FileName, 0);

         Assert.Equal(_responseReference, actual);
      }

      [Fact]
      public async Task TestListPathRequestsWithHttpVerb()
      {
         HttpMethod expected = HttpMethod.Get;
         await _sut.ListPathAsync(FilesystemName, DirectoryName, true, 1000);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.Method == expected), CancellationToken.None));
      }

      [Fact]
      public async Task TestListPathRequestsWithUriForRecursive()
      {
         var expected =
            new Uri(
               $"https://{StorageAccountName}.dfs.core.windows.net/{FilesystemName}?directory={DirectoryName}&maxresults=1000&recursive=true&resource=filesystem&timeout=60");
         await _sut.ListPathAsync(FilesystemName, DirectoryName, true, 1000);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.RequestUri == expected), CancellationToken.None));
      }

      [Fact]
      public async Task TestListPathRequestsWithUriForNonRecursive()
      {
         var expected =
            new Uri(
               $"https://{StorageAccountName}.dfs.core.windows.net/{FilesystemName}?directory={DirectoryName}&maxresults=1000&recursive=false&resource=filesystem&timeout=60");
         await _sut.ListPathAsync(FilesystemName, DirectoryName, false, 1000);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.RequestUri == expected), CancellationToken.None));
      }

      [Fact]
      public async Task TestListPathRequestsWithUriForMaxResults()
      {
         var expected =
            new Uri(
               $"https://{StorageAccountName}.dfs.core.windows.net/{FilesystemName}?directory={DirectoryName}&maxresults=2000&recursive=true&resource=filesystem&timeout=60");
         await _sut.ListPathAsync(FilesystemName, DirectoryName, true, 2000);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.RequestUri == expected), CancellationToken.None));
      }

      [Fact]
      public async Task TestListPathRequestsWithSignatureForRecursive()
      {
         string expected =
            $"GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}\ndirectory:{DirectoryName}\nmaxresults:1000\nrecursive:true\nresource:filesystem\ntimeout:60";
         await _sut.ListPathAsync(FilesystemName, DirectoryName, true, 1000);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestListPathRequestsWithSignatureForNonRecursive()
      {
         string expected =
            $"GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}\ndirectory:{DirectoryName}\nmaxresults:1000\nrecursive:false\nresource:filesystem\ntimeout:60";
         await _sut.ListPathAsync(FilesystemName, DirectoryName, false, 1000);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestListPathRequestsWithSignatureForMaxResults()
      {
         string expected =
            $"GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}\ndirectory:{DirectoryName}\nmaxresults:2000\nrecursive:true\nresource:filesystem\ntimeout:60";
         await _sut.ListPathAsync(FilesystemName, DirectoryName, true, 2000);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestListPathRequestsWithAuthorisationHeader()
      {
         await _sut.ListPathAsync(FilesystemName, DirectoryName, true, 1000);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.Authorization.Equals(_authenticationHeaderValue)
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestListPathRequestsWithMsDateHeader()
      {
         await _sut.ListPathAsync(FilesystemName, DirectoryName, true, 1000);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-date").Value.First() == _dateTimeReference.ToString("R")
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestListPathRequestsWithMsVersionHeader()
      {
         await _sut.ListPathAsync(FilesystemName, DirectoryName, true, 1000);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-version").Value.First() == "2018-11-09"
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestListPathRequestsWithEmptyContent()
      {
         await _sut.ListPathAsync(FilesystemName, DirectoryName, true, 1000);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Content.Headers.ContentLength == 0
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestListPathReturnsResponse()
      {
         HttpResponseMessage actual = await _sut.ListPathAsync(FilesystemName, DirectoryName, true, 1000);

         Assert.Equal(_responseReference, actual);
      }

      [Fact]
      public async Task TestReadPathRequestsWithHttpVerb()
      {
         HttpMethod expected = HttpMethod.Get;
         await _sut.ReadPathAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.Method == expected), CancellationToken.None));
      }

      [Fact]
      public async Task TestReadPathRequestsWithUri()
      {
         var expected =
            new Uri(
               $"https://{StorageAccountName}.dfs.core.windows.net/{FilesystemName}/{FileName}?timeout=60");
         await _sut.ReadPathAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y => y.RequestUri == expected), CancellationToken.None));
      }

      [Fact]
      public async Task TestReadPathRequestsWithSignatureForFullRange()
      {
         string expected =
            $"GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}/{Uri.EscapeDataString(FileName)}\ntimeout:60";
         await _sut.ReadPathAsync(FilesystemName, FileName);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestReadPathRequestsWithSignatureForRange()
      {
         string expected =
            $"GET\n\n\n\n\n\n\n\n\n\n\nbytes=10-15\nx-ms-date:{_dateTimeReference:R}\nx-ms-version:2018-11-09\n/{StorageAccountName}/{FilesystemName}/{Uri.EscapeDataString(FileName)}\ntimeout:60";
         await _sut.ReadPathAsync(FilesystemName, FileName, 10, 15);

         _authorisation.Verify(x => x.AuthoriseAsync(StorageAccountName, expected));
      }

      [Fact]
      public async Task TestReadPathRequestsWithAuthorisationHeader()
      {
         await _sut.ReadPathAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.Authorization.Equals(_authenticationHeaderValue)
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestReadPathRequestsWithMsDateHeader()
      {
         await _sut.ReadPathAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-date").Value.First() == _dateTimeReference.ToString("R")
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestReadPathRequestsWithMsVersionHeader()
      {
         await _sut.ReadPathAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Headers.First(z => z.Key == "x-ms-version").Value.First() == "2018-11-09"
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestReadPathRequestsWithEmptyContent()
      {
         await _sut.ReadPathAsync(FilesystemName, FileName);

         _httpClient.Verify(x =>
            x.SendAsync(It.Is<HttpRequestMessage>(y =>
               y.Content.Headers.ContentLength == 0
            ), CancellationToken.None));
      }

      [Fact]
      public async Task TestReadPathReturnsResponse()
      {
         HttpResponseMessage actual = await _sut.ReadPathAsync(FilesystemName, FileName);

         Assert.Equal(_responseReference, actual);
      }
   }
}