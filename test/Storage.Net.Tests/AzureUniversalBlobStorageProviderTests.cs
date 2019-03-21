using Microsoft.WindowsAzure.Storage.Blob;
using Storage.Net.Blob;
using Storage.Net.Microsoft.Azure.Storage.Blob;
using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace Storage.Net.Tests
{
    public class AzureUniversalBlobStorageProviderTests
    {
        [Theory]
        [InlineData("")]
        [InlineData("xpto")]
        [InlineData("xpto/")]
        public async Task WriteTextAsyncThrowsOnInvalidId(string id)
        {
            IBlobStorage blobStorage = new AzureUniversalBlobStorageProvider(new CloudBlobClient(new UriBuilder("garbage").Uri));

            await Assert.ThrowsAsync<ArgumentException>(() => blobStorage.WriteTextAsync(id, string.Empty));
        }

        [Theory]
        [InlineData("")]
        [InlineData("xpto")]
        [InlineData("xpto/")]
        public async Task WriteFileAsyncThrowsOnInvalidId(string id)
        {
            IBlobStorage blobStorage = new AzureUniversalBlobStorageProvider(new CloudBlobClient(new UriBuilder("garbage").Uri));

            await Assert.ThrowsAsync<ArgumentException>(() => blobStorage.WriteFileAsync(id, string.Empty));
        }

        [Theory]
        [InlineData("")]
        [InlineData("xpto")]
        [InlineData("xpto/")]
        public async Task WriteAsyncThrowsOnInvalidId(string id)
        {
            // We don't care about the implementation because we wont actually connect to BlobStorage
            IBlobStorage blobStorage = new AzureUniversalBlobStorageProvider(new CloudBlobClient(new UriBuilder("garbage").Uri));

            await Assert.ThrowsAsync<ArgumentException>(() => blobStorage.WriteAsync(id, new MemoryStream()));
        }
    }
}
