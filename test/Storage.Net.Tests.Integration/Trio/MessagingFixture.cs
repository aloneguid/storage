﻿using Storage.Net.Messaging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Config.Net;
using System.Threading;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.IO;
using System.Reflection;

namespace Storage.Net.Tests.Integration.Messaging
{
   public abstract class MessagingFixture : IDisposable
   {
      private const string TagPropertyName = "tag";
      private static readonly TimeSpan MaxWaitTime = TimeSpan.FromMinutes(5);
      private static readonly ITestSettings _settings;
      public readonly IMessagePublisher Publisher;
      public readonly IMessageReceiver Receiver;
      private readonly ConcurrentDictionary<string, QueueMessage> _receivedMessages = new ConcurrentDictionary<string, QueueMessage>();
      private QueueMessage _lastReceivedMessage;
      private int _receivedCount;

      private bool _pumpStarted = false;
      private readonly CancellationTokenSource _cts = new CancellationTokenSource();
      protected readonly string _testDir;

      static MessagingFixture()
      {
         _settings = new ConfigurationBuilder<ITestSettings>()
            .UseIniFile("c:\\tmp\\integration-tests.ini")
            .UseEnvironmentVariables()
            .Build();
      }

      public MessagingFixture()
      {
         string buildDir = new FileInfo(new Uri(Assembly.GetExecutingAssembly().CodeBase).LocalPath).Directory.FullName;
         _testDir = Path.Combine(buildDir, "TEST-" + Guid.NewGuid());
         Directory.CreateDirectory(_testDir);

         Publisher = CreatePublisher(_settings);
         Receiver = CreateReceiver(_settings);
      }

      protected abstract IMessagePublisher CreatePublisher(ITestSettings settings);

      protected abstract IMessageReceiver CreateReceiver(ITestSettings settings);

      public async Task StartPumpAsync()
      {
         if(_pumpStarted || Receiver == null)
            return;

         _pumpStarted = true;

         //start the pump
         await Receiver.StartMessagePumpAsync(ReceiverPumpAsync, cancellationToken: _cts.Token, maxBatchSize: 500).ConfigureAwait(false);
      }

      public async Task<string> PutMessageAsync(QueueMessage message)
      {
         string tag = Guid.NewGuid().ToString();
         message.Properties[TagPropertyName] = tag;

         Log("submitting tag {0}", tag);

         try
         {
            await Publisher.PutMessagesAsync(new[] { message }).ConfigureAwait(false);
         }
         catch(Exception ex)
         {
            Log(ex.ToString());
         }

         return tag;
      }

      public async Task<QueueMessage> WaitMessageAsync(string tag, int minCount = 1)
      {
         DateTime start = DateTime.UtcNow;

         while((DateTime.UtcNow - start) < MaxWaitTime)
         {
            QueueMessage candidate = GetTaggedMessage(tag);

            if(candidate != null)
            {
               Log("found tagged candidate " + tag);
            }

            if(candidate != null && GetMessageCount() >= minCount)
            {
               return candidate;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(300)).ConfigureAwait(false);
         }

         return null;
      }


      private void Log(string format, params object[] parameters)
      {
         string date = DateTime.UtcNow.ToString();
         Debug.WriteLine(string.Format(date + " DBG: " + format, parameters));
         Console.WriteLine(date + " CON: " + format, parameters);
      }

      private async Task ReceiverPumpAsync(IReadOnlyCollection<QueueMessage> messages)
      {
         foreach(QueueMessage qm in messages)
         {
            qm.Properties.TryGetValue(TagPropertyName, out string tag);

            if(tag != null)
            {
               Log("received tag: {0}", tag);

               _receivedMessages.TryAdd(tag, qm);
            }

            _lastReceivedMessage = qm;
            Interlocked.Increment(ref _receivedCount);
         }

         try
         {
            await Receiver.ConfirmMessagesAsync(messages);
         }
         catch(NotSupportedException)
         {
            //some provides may not support this
         }
         catch(Exception ex)
         {
            Log(ex.ToString());
         }
      }

      public QueueMessage GetTaggedMessage(string tag)
      {
         if(tag == null)
            return _lastReceivedMessage;

         if(!_receivedMessages.TryGetValue(tag, out QueueMessage result))
            return null;

         return result;
      }

      public int GetMessageCount() => _receivedCount;

      public void Dispose()
      {
         _cts.Cancel();

         if(Publisher != null)
            Publisher.Dispose();

         if(Receiver != null)
            Receiver.Dispose();
      }
   }
}