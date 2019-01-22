using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Storage.Net.Messaging
{
   /// <summary>
   /// Message to be used in all the queueing code
   /// </summary>
   public class QueueMessage
   {
      private Dictionary<string, string> _properties;

      /// <summary>
      /// Creates an instance of <see cref="QueueMessage"/>
      /// </summary>
      /// <param name="id">Message ID</param>
      /// <param name="content">Message content</param>
      public QueueMessage(string id, string content)
      {
         Id = id;
         StringContent = content;
      }

      /// <summary>
      /// Creates an instance of <see cref="QueueMessage"/>
      /// </summary>
      /// <param name="id">Message ID</param>
      /// <param name="content">Message content</param>
      public QueueMessage(string id, byte[] content)
      {
         Id = id;
         Content = content;
      }

      /// <summary>
      /// Create queue message from message content
      /// </summary>
      /// <param name="content"></param>
      public QueueMessage(string content) : this(null, content)
      {
         
      }

      /// <summary>
      /// Create queue message from message content
      /// </summary>
      /// <param name="content"></param>
      public QueueMessage(byte[] content) : this(null, content)
      {

      }

      /// <summary>
      /// Creates a new queue message from a text string
      /// </summary>
      /// <param name="text">Text to set as message content</param>
      /// <returns>New queue message</returns>
      public static QueueMessage FromText(string text)
      {
         if (text == null) throw new ArgumentNullException(nameof(text));

         return new QueueMessage(text);
      }

      /// <summary>
      /// Message ID
      /// </summary>
      public string Id { get; set; }

      /// <summary>
      /// Gets the count of how many time this message was dequeued
      /// </summary>
      public int DequeueCount { get; set; }

      /// <summary>
      /// Message content as string
      /// </summary>
      public string StringContent
      {
         get { return Content == null ? null : Encoding.UTF8.GetString(Content, 0, Content.Length); }
         set
         {
            if (string.IsNullOrEmpty(value)) Content = null;
            else Content = Encoding.UTF8.GetBytes(value);
         }
      }

      /// <summary>
      /// Message content as byte array
      /// </summary>
      public byte[] Content { get; set; }

      /// <summary>
      /// Extra properties for this message
      /// </summary>
      public Dictionary<string, string> Properties
      {
         get
         {
            if(_properties == null) _properties = new Dictionary<string, string>();
            return _properties;
         }
      }

      /// <summary>
      /// Clones the message
      /// </summary>
      /// <returns></returns>
      public QueueMessage Clone()
      {
         var message = new QueueMessage(Id, Content);
         if (_properties != null) message._properties = new Dictionary<string, string>(_properties);
         return message;
      }

      /// <summary>
      /// Extremely compact binary representation of the message.
      /// It's library specific, therefore try not to use it if portability is required.
      /// </summary>
      /// <returns></returns>
      public byte[] ToByteArray()
      {
         using (var ms = new MemoryStream())
         {
            using (var b = new BinaryWriter(ms, Encoding.UTF8, true))
            {
               b.Write((byte)1); //version marker
               b.Write(Id != null);
               if (Id != null)
               {
                  b.Write(Id);
               }
               b.Write(DequeueCount);

               //Content
               if (Content == null)
               {
                  b.Write((int)0);
               }
               else
               {
                  b.Write(Content.Length);
                  b.Write(Content);
               }

               //properties
               if(_properties == null)
               {
                  b.Write((int)0);
               }
               else
               {
                  b.Write(_properties.Count);
                  foreach(KeyValuePair<string, string> pair in _properties)
                  {
                     b.Write(pair.Key);
                     b.Write(pair.Value != null);
                     if (pair.Value != null)
                     {
                        b.Write(pair.Value);
                     }
                  }
               }
            }

            return ms.ToArray();
         }
      }

      /// <summary>
      /// Constructs the message back from compact representation
      /// </summary>
      /// <param name="data">Binary data</param>
      /// <returns></returns>
      public static QueueMessage FromByteArray(byte[] data)
      {
         using (var ms = new MemoryStream(data))
         {
            using (var b = new BinaryReader(ms, Encoding.UTF8, true))
            {
               byte version = b.ReadByte();  //to be used with versioning
               if(version != 1)
               {
                  throw new ArgumentException($"version {version} is not supported", nameof(data));
               }

               var msg = new QueueMessage(
                  b.ReadBoolean() ? b.ReadString() : null,
                  (byte[])null);
               msg.DequeueCount = b.ReadInt32();
               int contentLength = b.ReadInt32();
               byte[] content = contentLength == 0 ? null : b.ReadBytes(contentLength);
               msg.Content = content;

               int propCount = b.ReadInt32();
               while(--propCount >= 0)
               {
                  string key = b.ReadString();
                  string value = b.ReadBoolean() ? b.ReadString() : null;
                  msg.Properties[key] = value;
               }

               return msg;
            }
         }

         throw new NotImplementedException();
      }
   }
}
