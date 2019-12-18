using System;
using System.Collections.Generic;
using Storage.Net.Blobs;
using System.Linq;
using Storage.Net.Messaging;

namespace Storage.Net.ConnectionString
{
   static class ConnectionStringFactory
   {
      private const string TypeSeparator = "://";
      private static readonly List<IConnectionFactory> Factories = new List<IConnectionFactory>();

      static ConnectionStringFactory()
      {
         Register(new BuiltInConnectionFactory());
      }

      public static void Register(IConnectionFactory factory)
      {
         if (factory == null) throw new ArgumentNullException(nameof(factory));

         Factories.Add(factory);
      }

      public static IBlobStorage CreateBlobStorage(string connectionString)
      {
         return Create(connectionString, (factory, cs) => factory.CreateBlobStorage(cs));
      }

      public static IMessenger CreateMessager(string connectionString)
      {
         return Create(connectionString, (factory, cs) => factory.CreateMessenger(cs));
      }


      private static TInstance Create<TInstance>(string connectionString, Func<IConnectionFactory, StorageConnectionString, TInstance> createAction)
         where TInstance: class
      {
         if (connectionString == null)
         {
            throw new ArgumentNullException(nameof(connectionString));
         }

         var pcs = new StorageConnectionString(connectionString);

         TInstance instance = Factories
            .Select(f => createAction(f, pcs))
            .FirstOrDefault(b => b != null);

         if (instance == null)
         {
            throw new ArgumentException(
               $"could not create any implementation based on the passed connection string (prefix: {pcs.Prefix}), did you register required external module?",
               nameof(connectionString));
         }

         return instance;
      }

   }
}
