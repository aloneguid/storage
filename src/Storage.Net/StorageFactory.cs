namespace Storage.Net
{
   /// <summary>
   /// Helper syntax for creating instances of storage library objects
   /// </summary>
   public static class StorageFactory
   {
      private static readonly IKeyValueStorageFactory _tables = new InternalTablesFactory();
      private static readonly IBlobStorageFactory _blobs = new InternalBlobsFactory();
      private static readonly IMessagingFactory _messages = new InternalMessagingFactory();
      private static readonly IModulesFactory _moduleInit = new InternalModuleInitFactory();
      private static readonly IConnectionStringFactory _css = new InternalConnectionStringFactory();

      /// <summary>
      /// Access to creating tables
      /// </summary>
      public static IKeyValueStorageFactory KeyValue => _tables;

      /// <summary>
      /// Access to creating blobs
      /// </summary>
      public static IBlobStorageFactory Blobs => _blobs;

      /// <summary>
      /// Access to creating messaging
      /// </summary>
      public static IMessagingFactory Messages => _messages;

      /// <summary>
      /// Module initialisation
      /// </summary>
      public static IModulesFactory Modules => _moduleInit;

      /// <summary>
      /// Connection strings
      /// </summary>
      public static IConnectionStringFactory ConnectionStrings => _css;

      class InternalTablesFactory : IKeyValueStorageFactory
      {
      }

      class InternalBlobsFactory : IBlobStorageFactory
      {
      }

      class InternalMessagingFactory : IMessagingFactory
      {

      }

      class InternalModuleInitFactory : IModulesFactory
      {

      }

      class InternalConnectionStringFactory : IConnectionStringFactory
      {

      }
   }

   /// <summary>
   /// Crates blob storage implementations
   /// </summary>
   public interface IBlobStorageFactory
   {
   }

   /// <summary>
   /// Creates messaging implementations
   /// </summary>
   public interface IMessagingFactory
   {
   }

   /// <summary>
   /// Crates table storage implementations
   /// </summary>
   public interface IKeyValueStorageFactory
   {
   }

   /// <summary>
   /// Creates connection strings, acts as a helper
   /// </summary>
   public interface IConnectionStringFactory
   {

   }

   /// <summary>
   /// Module initialisation primitives
   /// </summary>
   public interface IModulesFactory
   {

   }

}