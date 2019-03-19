using Storage.Net.Blob;
using Storage.Net.Microsoft.Azure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Storage.Net.Tests.Integration.Azure
{
    public class LeakyAzureBlobStorageTest
    {
#if DEBUG
        //[Fact]
        public async Task Read_large_file()
        {
            IBlobStorage blobsGeneric = StorageFactory.Blobs.AzureBlobStorage("", "");

            var blobsAzure = (IAzureBlobStorageNativeOperations)blobsGeneric;

            Stream s = await blobsAzure.OpenRandomAccessReadAsync("large/bee1.zip");

            //s.Seek(-(s.Length - 20), SeekOrigin.End);
            byte[] data = new byte[100];
            int read = s.Read(data, 0, 20);
        }
#endif
    }
}
