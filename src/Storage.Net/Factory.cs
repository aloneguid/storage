using System;
using System.IO;
using Storage.Net.Blobs;
using Storage.Net.Blobs.Files;
using Storage.Net.Messaging;
using Storage.Net.ConnectionString;
using Storage.Net.Messaging.Large;
using Storage.Net.Messaging.Files;
using System.IO.Compression;
using Storage.Net.Blobs.Sinks.Impl;
using Storage.Net.Blobs.Sinks;

namespace Storage.Net
{
   /// <summary>
   /// Factory extension methods
   /// </summary>
   public static class Factory
   {
      /// <summary>
      /// Call to initialise a module
      /// </summary>
      public static IModulesFactory Use(this IModulesFactory factory, IExternalModule module)
      {
         if (module == null)
         {
            throw new ArgumentNullException(nameof(module));
         }

         IConnectionFactory connectionFactory = module.ConnectionFactory;
         if (connectionFactory != null)
         {
            ConnectionStringFactory.Register(connectionFactory);
         }

         return factory;
      }

      /// <summary>
      /// Creates a blob stogage instance from a connection string
      /// </summary>
      public static IBlobStorage FromConnectionString(this IBlobStorageFactory factory, string connectionString)
      {
         return ConnectionStringFactory.CreateBlobStorage(connectionString);
      }

      /// <summary>
      /// Creates message publisher
      /// </summary>
      public static IMessenger MessengerFromConnectionString(this IMessagingFactory factory, string connectionString)
      {
         return ConnectionStringFactory.CreateMessager(connectionString);
      }

      /// <summary>
      /// Creates an instance in a specific disk directory
      /// <param name="factory"></param>
      /// <param name="directoryFullName">Root directory</param>
      /// </summary>
      public static IBlobStorage DirectoryFiles(this IBlobStorageFactory factory,
         string directoryFullName)
      {
         return new DiskDirectoryBlobStorage(directoryFullName);
      }

      /// <summary>
      /// Zip file
      /// </summary>
      public static IBlobStorage ZipFile(this IBlobStorageFactory blobStorageFactory, string filePath)
      {
         return new ZipFileBlobStorage(filePath);
      }

      /// <summary>
      /// Creates an instance of blob storage which stores everyting in memory. Useful for testing purposes only or if blobs don't
      /// take much space.
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <returns>In-memory blob storage instance</returns>
      public static IBlobStorage InMemory(this IBlobStorageFactory factory)
      {
         return new InMemoryBlobStorage();
      }

      /// <summary>
      /// Creates a virtual storage where you can mount other storage providers to a specific virtual directory
      /// </summary>
      /// <param name="factory"></param>
      /// <returns></returns>
      public static IVirtualStorage Virtual(this IBlobStorageFactory factory)
      {
         return new VirtualStorage();
      }

      /// <summary>
      /// Creates a message publisher which holds messages in memory.
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="name">Memory buffer name. Publishers with the same name will contain identical messages. Querying a publisher again
      /// with the same name returns an identical publisher. To create a receiver for this memory bufffer use the same name.</param>
      public static IMessenger InMemory(this IMessagingFactory factory, string name)
      {
         return InMemoryMessenger.CreateOrGet(name);
      }

      /// <summary>
      /// Creates a message publisher that uses local disk directory as a backing store
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="path">Path to directory to use as a backing store. If it doesn't exist, it will be created.</param>
      /// <returns></returns>
      public static IMessenger Disk(this IMessagingFactory factory, string path)
      {
         return new LocalDiskMessenger(path);
      }

      /// <summary>
      /// Wraps message publisher so that if it's content is larger than <paramref name="minSizeLarge"/>, the content is uploaded
      /// to blob storage and cleared on the message itself. The message is then stamped with a property <see cref="QueueMessage.LargeMessageContentHeaderName"/>
      /// which contains blob path of the message content.
      /// </summary>
      /// <param name="messenger">Message publisher to wrap</param>
      /// <param name="offloadStorage">Blob storage used to offload the message content</param>
      /// <param name="minSizeLarge">Threshold size</param>
      /// <param name="blobPathGenerator">Optional generator for blob path used to save large message content.</param>
      /// <returns></returns>
      public static IMessenger HandleLargeContent(this IMessenger messenger, IBlobStorage offloadStorage, int minSizeLarge,
         Func<QueueMessage, string> blobPathGenerator = null)
      {
         return new LargeMessageMessenger(messenger, offloadStorage, minSizeLarge, blobPathGenerator, false);
      }

      #region [ Data Decorators ]

      /// <summary>
      /// 
      /// </summary>
      /// <param name="blobStorage"></param>
      /// <param name="sinks"></param>
      /// <returns></returns>
      public static IBlobStorage WithSinks(this IBlobStorage blobStorage,
         params ITransformSink[] sinks)
      {
         return new SinkedBlobStorage(blobStorage, sinks);
      }

      /// <summary>
      /// Wraps blob storage into zip compression
      /// </summary>
      /// <param name="blobStorage"></param>
      /// <param name="compressionLevel"></param>
      /// <returns></returns>
      public static IBlobStorage WithGzipCompression(
         this IBlobStorage blobStorage, CompressionLevel compressionLevel = CompressionLevel.Optimal)
      {
         return blobStorage.WithSinks(new GZipSink(compressionLevel));
      }

#if !NET16

      /// <summary>
      /// 
      /// </summary>
      /// <param name="blobStorage"></param>
      /// <param name="encryptionKey"></param>
      /// <returns></returns>
      public static IBlobStorage WithSymmetricEncryption(
         this IBlobStorage blobStorage,
         string encryptionKey)
      {
         return blobStorage.WithSinks(new SymmetricEncryptionSink(encryptionKey));
      }

#endif

      #endregion
   }
}
