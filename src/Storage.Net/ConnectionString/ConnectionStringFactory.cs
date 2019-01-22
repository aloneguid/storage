using System;
using System.Collections.Generic;
using Storage.Net.Blob;
using NetBox.Extensions;
using System.Linq;
using Storage.Net.KeyValue;
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

      public static IKeyValueStorage CreateKeyValueStorage(string connectionString)
      {
         return Create(connectionString, (factory, cs) => factory.CreateKeyValueStorage(cs));
      }

      public static IMessagePublisher CreateMessagePublisher(string connectionString)
      {
         return Create(connectionString, (factory, cs) => factory.CreateMessagePublisher(cs));
      }

      public static IMessageReceiver CreateMessageReceiver(string connectionString)
      {
         return Create(connectionString, (factory, cs) => factory.CreateMessageReceiver(cs));
      }


      private static TInstance Create<TInstance>(string connectionString, Func<IConnectionFactory, StorageConnectionString, TInstance> createAction)
      {
         if (connectionString == null)
         {
            throw new ArgumentNullException(nameof(connectionString));
         }

         var pcs = new StorageConnectionString(connectionString);

         TInstance instance = Factories
            .Select(f => createAction(f, pcs))
            .Where(b => b != null)
            .FirstOrDefault();

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
