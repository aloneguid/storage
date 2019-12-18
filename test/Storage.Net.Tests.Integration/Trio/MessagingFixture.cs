using Storage.Net.Messaging;
using System;
using Config.Net;
using System.IO;
using System.Reflection;

namespace Storage.Net.Tests.Integration.Messaging
{
   public abstract class MessagingFixture : IDisposable
   {
      private static readonly ITestSettings _settings = new ConfigurationBuilder<ITestSettings>()
            .UseIniFile("c:\\tmp\\integration-tests.ini")
            .UseEnvironmentVariables()
            .Build();
      public readonly IMessenger Messenger;
      private readonly string _fixtureName;
      protected readonly string _testDir;

      protected MessagingFixture()
      {
         _fixtureName = GetType().Name;
         string buildDir = new FileInfo(new Uri(Assembly.GetExecutingAssembly().CodeBase).LocalPath).Directory.FullName;
         _testDir = Path.Combine(buildDir, "msg-" + Guid.NewGuid());
         Directory.CreateDirectory(_testDir);

         Messenger = CreateMessenger(_settings);
      }

      protected abstract IMessenger CreateMessenger(ITestSettings settings);

      public void Dispose()
      {
         if(Messenger != null) 
            Messenger.Dispose();

         Directory.Delete(_testDir, true);
      }
   }
}