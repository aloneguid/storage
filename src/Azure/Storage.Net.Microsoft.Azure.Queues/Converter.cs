using Microsoft.Azure.Storage.Queue;
using NetBox.Extensions;
using Storage.Net.Messaging;
using System;
using System.IO;
using System.Linq;
using System.Text;

namespace Storage.Net.Microsoft.Azure.Queues
{
   internal static class Converter
   {
      private const string PropEndWord = "PROPEND";
      private static readonly Guid CustomFlag = new Guid("820e7dc0-46a3-4177-a241-cdac97275ca9");
      private static readonly byte[] CustomFlagBytes = CustomFlag.ToByteArray();

      public static CloudQueueMessage ToCloudQueueMessage(QueueMessage message)
      {
         if(message == null)
            throw new ArgumentNullException(nameof(message));

         //when there are no properties pack the data as binary in raw form
         if(message.Properties == null || message.Properties.Count == 0)
         {
            var r = new CloudQueueMessage((string)null);
            r.SetMessageContent2(message.Content);
            return r;
         }

         //note that Azure Storage doesn't have properties on message, therefore I can do a simulation instead

         var clazz = new JsonProps
         {
            Properties = message.Properties.Select(p => new JsonProp { Name = p.Key, Value = p.Value }).ToArray()
         };
         byte[] propBytes = Encoding.UTF8.GetBytes(clazz.ToJsonString());

         CloudQueueMessage result;
         using (var ms = new MemoryStream())
         {
            using (var writer = new BinaryWriter(ms, Encoding.UTF8))
            {
               writer.Write(CustomFlagBytes);
               writer.Write(propBytes.Length);
               writer.Write(propBytes);
               if (message.Content != null)
               {
                  writer.Write(message.Content);
               }
            }

            result = new CloudQueueMessage((string)null);
            result.SetMessageContent2(ms.ToArray());
         }

         return result;
      }

      public static QueueMessage ToQueueMessage(CloudQueueMessage message)
      {
         if(message == null) return null;

         byte[] mb = message.AsBytes;
         QueueMessage result;
         if (!IsCustomMessage(mb))
         {
            result = new QueueMessage(CreateId(message), mb);
         }
         else
         {
            using (var ms = new MemoryStream(mb))
            {
               //skip forward custom message flag
               ms.Seek(CustomFlagBytes.Length, SeekOrigin.Begin);

               //read the custom properties length
               int cpl;
               using (var br = new BinaryReader(ms, Encoding.UTF8, true))
               {
                  cpl = br.ReadInt32();
               }

               //read the actual properties
               byte[] propBytes = new byte[cpl];
               ms.Read(propBytes, 0, cpl);
               string propString = Encoding.UTF8.GetString(propBytes);
               JsonProps props = propString.AsJsonObject<JsonProps>();

               //read message data
               byte[] leftovers = ms.ToByteArray();

               result = new QueueMessage(CreateId(message), leftovers);
               foreach (JsonProp prop in props.Properties)
               {
                  result.Properties[prop.Name] = prop.Value;
               }
            }
         }

         result.DequeueCount = message.DequeueCount;
         return result;
      }

      private static bool IsCustomMessage(byte[] messageBytes)
      {
         if(messageBytes.Length < CustomFlagBytes.Length) return false;

         byte[] firstBytes = new byte[CustomFlagBytes.Length];
         Array.Copy(messageBytes, 0, firstBytes, 0, CustomFlagBytes.Length);

         return firstBytes.SequenceEqual(CustomFlagBytes);
      }

      internal static string CreateId(CloudQueueMessage message)
      {
         if(string.IsNullOrEmpty(message.PopReceipt)) return message.Id;

         return message.Id + ":" + message.PopReceipt;
      }

      internal static void SplitId(string compositeId, out string id, out string popReceipt)
      {
         string[] parts = compositeId.Split(new[] { ':' }, 2, StringSplitOptions.RemoveEmptyEntries);
         id = parts[0];
         popReceipt = parts.Length > 1 ? parts[1] : null;
      }

   }
}
