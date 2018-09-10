namespace Storage.Net
{
   /// <summary>
   /// Helper syntax for creating instances of storage library objects
   /// </summary>
   public static class StorageFactory
   {
      private static IKeyValueStorageFactory _tables = new InternalTablesFactory();
      private static IBlobStorageFactory _blobs = new InternalBlobsFactory();
      private static IMessagingFactory _messages = new InternalMessagingFactory();
      private static IModulesFactory _moduleInit = new InternalModuleInitFactory();

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
   /// Module initialisation primitives
   /// </summary>
   public interface IModulesFactory
   {

   }

}