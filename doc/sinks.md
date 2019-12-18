# Data Transformation Sinks

Index
- Available Sinks
  - [GZip](#gzip-compression)
  - [Symmetric Encryption](#symmetric-encryption)
- [Build Your Own](#build-your-own-sink)

## Available Sinks

## Gzip Compression

To create the sink, call extension method `WithGzipCompression` and optionally pass a *compression level* which defaults to `Optimal`:

```csharp
IBlobStorage storage = StorageFactory.Blobs
   .XXX()
   .WithGzipCompression(CompressionLevel compressionLevel = CompressionLevel.Optimal)
```

## Symmetric Encryption

This sink implements [symmetric encryption](https://www.venafi.com/blog/what-symmetric-encryption) for upload/download data. I.e. uploaded data is encrypted with a key, and decrypted after download.

It uses [Rijndael](https://web.archive.org/web/20070711123800/http://csrc.nist.gov/CryptoToolkit/aes/rijndael/Rijndael-ammended.pdf) encryption with default settings, which is a superset of **AES** encryption algorithm (read about [differences](https://stackoverflow.com/a/748645/80858)). For each encryption session (blob upload) *a new initialisation vector is created*. 

To add:

```csharp
IBlobStorage storage = StorageFactory.Blobs
   .XXX()
   .WithSymmetricEncryption(string encryptionKey)
```

The encryption key is a baase64 encoded binary key. To generate it, you can use the following snippet:

```csharp
void Main()
{
	var cs = new RijndaelManaged();
	cs.GenerateKey();
	string keyBase64 = Convert.ToBase64String(cs.Key);
	
	Console.WriteLine("new encryption key:" + keyBase64);
}
```

Note that it's your own responsibility to store the key securely, make sure it's not put in plaintext anywhere it can be stoken from!


> the list of available sinks will be growing, this is a new functionality!

## Build Your Own Sink

Implementing your own transformation sink is a matter of implementing a new class derived from [`ITransformSink`](../src/Storage.Net/Blobs/Sinks/ITransformSink.cs) interface, which only has two methods:

```csharp
   public interface ITransformSink
   {
      Stream OpenReadStream(string fullPath, Stream parentStream);

      Stream OpenWriteStream(string fullPath, Stream parentStream);
   }
```

The first one is called when Storage.Net opens a blob for reading, so that you can replace original stream passed in `parentStream` with your own. The second one does the reverse. For instance, have a look at the implementation of `Gzip` sink, as it's the easiest one:

```csharp
public class GZipSink : ITransformSink
{
   private readonly CompressionLevel _compressionLevel;

   public GZipSink(CompressionLevel compressionLevel = CompressionLevel.Optimal)
   {
      _compressionLevel = compressionLevel;
   }

   public Stream OpenReadStream(string fullPath, Stream parentStream)
   {
      if(parentStream == null)
         return null;

      return new GZipStream(parentStream, CompressionMode.Decompress, false);
   }

   public Stream OpenWriteStream(string fullPath, Stream parentStream)
   {
      return new GZipStream(parentStream, _compressionLevel, false);
   }
}
```

This sink simply takes incoming stream and wraps it around in the standard built-in `GZipStream` from `System.IO.Compression` namespace.

### Passing Your Sink To Storage

In order to use the sink, you can simply call `.WithSinks` extension method and pass the sink you want to use. For instance, to enable GZipSink do the following:

```csharp
IBlobStorage storage = StorageFactory.Blobs
   .XXX()
   .WithSinks(new GZipSink());
```

You can also create an extension method if you use this often:

```csharp
public static IBlobStorage WithGzipCompression(
   this IBlobStorage blobStorage, CompressionLevel compressionLevel = CompressionLevel.Optimal)
{
   return blobStorage.WithSinks(new GZipSink(compressionLevel));
}
```

### Chaining Sinks

`.WithSinks` extension method in fact accept an array of sinks, which means that sinks can be chained together. This is useful when you need to do multiple transformations at the same time. For instance, if I would like to both compress, and encrypt data in the target storage, I could initialise my storage in the following way:

```csharp
IBlobStorage encryptedAndCompressed =          
   StorageFactory.Blobs
      .InMemory()
      .WithSinks(
         new GZipSink(),
         new SymmetricEncryptionSink("To6X5XVaNNMKFfxssJS6biREGpOVZjEIC6T7cc1rJF0=")))
```

Note that declaration order matters here - when writing, the data is **compressed first**, and **encrypted second**.