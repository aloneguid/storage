using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Storage.Net.Blobs
{
   /// <summary>
   /// Blob item description
   /// </summary>
   public sealed class Blob : IEquatable<Blob>, ICloneable
   {
      /// <summary>
      /// Gets the kind of item
      /// </summary>
      public BlobItemKind Kind { get; private set; }

      /// <summary>
      /// Simply checks if kind of this item is <see cref="BlobItemKind.Folder"/>
      /// </summary>
      public bool IsFolder => Kind == BlobItemKind.Folder;

      /// <summary>
      /// Simply checks if kind of this item is <see cref="BlobItemKind.File"/>
      /// </summary>
      public bool IsFile => Kind == BlobItemKind.File;

      /// <summary>
      /// Gets the folder path containing this item
      /// </summary>
      public string FolderPath { get; private set; }

      /// <summary>
      /// Gets the name of this blob, uniqueue within the folder. In most providers this is the same as file name.
      /// </summary>
      public string Name { get; private set; }

      /// <summary>
      /// Blob size
      /// </summary>
      public long? Size { get; set; }

      /// <summary>
      /// MD5 content hash of the blob. Note that this property can be null if underlying storage has
      /// no information about the hash, or it's very expensive to calculate it, for instance it would require
      /// getting a whole content of the blob to hash it.
      /// </summary>
      public string MD5 { get; set; }

      /// <summary>
      /// Last modification time when known
      /// </summary>
      public DateTimeOffset? LastModificationTime { get; set; }

      /// <summary>
      /// Gets full path to this blob which is a combination of folder path and blob name
      /// </summary>
      public string FullPath => StoragePath.Combine(FolderPath, Name);

      /// <summary>
      /// Custom provider-specific properties. Key names are case-insensitive.
      /// </summary>
      public Dictionary<string, object> Properties { get; private set; } = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

      /// <summary>
      /// Try to get property and cast it to a specified type
      /// </summary>
      public bool TryGetProperty<TValue>(string name, out TValue value, TValue defaultValue = default)
      {
         if(name == null || !Properties.TryGetValue(name, out object objValue))
         {
            value = defaultValue;
            return false;
         }

         if(objValue is TValue)
         {
            value = (TValue)objValue;
            return true;
         }

         value = defaultValue;
         return false;
      }

      /// <summary>
      /// User defined metadata. Key names are case-insensitive.
      /// </summary>
      public Dictionary<string, string> Metadata { get; private set;  } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

      internal object Tag { get; set; }

      /// <summary>
      /// Tries to add properties in pairs when value is not null
      /// </summary>
      /// <param name="keyValues"></param>
      public void TryAddProperties(params object[] keyValues)
      {
         for(int i = 0; i < keyValues.Length; i += 2)
         {
            string key = (string)keyValues[i];
            object value = keyValues[i + 1];

            if(key != null && value != null)
            {
               if(value is string s && string.IsNullOrEmpty(s))
                  continue;

               Properties[key] = value;
            }
         }
      }

      /// <summary>
      /// Tries to add properties from dictionary by key names
      /// </summary>
      /// <param name="source"></param>
      /// <param name="keyNames"></param>
      public void TryAddPropertiesFromDictionary(IDictionary<string, string> source, params string[] keyNames)
      {
         if(source == null || keyNames == null)
            return;

         foreach(string key in keyNames)
         {
            if(source.TryGetValue(key, out string value))
            {
               Properties[key] = value;
            }
         }
      }

      /// <summary>
      /// Create a new instance
      /// </summary>
      /// <param name="fullPath"></param>
      /// <param name="kind"></param>
      public Blob(string fullPath, BlobItemKind kind = BlobItemKind.File)
      {
         SetFullPath(fullPath);

         Kind = kind;
      }

      /// <summary>
      /// Creates a new instance
      /// </summary>
      /// <param name="folderPath">Folder path to the blob</param>
      /// <param name="name">Name of the blob withing a specific folder.</param>
      /// <param name="kind">Blob kind (file or folder)</param>
      public Blob(string folderPath, string name, BlobItemKind kind)
      {
         Name = name ?? throw new ArgumentNullException(nameof(name));
         Name = StoragePath.NormalizePart(Name);
         FolderPath = StoragePath.Normalize(folderPath);
         Kind = kind;
      }

      /// <summary>
      /// Returns true if this item is a folder and it's a root folder
      /// </summary>
      public bool IsRootFolder => Kind == BlobItemKind.Folder && StoragePath.IsRootPath(FullPath);

      /// <summary>
      /// Full blob info, i.e type, id and path
      /// </summary>
      public override string ToString()
      {
         string k = Kind == BlobItemKind.File ? "file" : "folder";

         return $"{k}: {Name}@{FolderPath}";
      }

      /// <summary>
      /// Equality check
      /// </summary>
      /// <param name="other"></param>
      public bool Equals(Blob other)
      {
         if(ReferenceEquals(other, null))
            return false;

         return
            other.FullPath == FullPath &&
            other.Kind == Kind;
      }

      /// <summary>
      /// Equality check
      /// </summary>
      /// <param name="other"></param>
      public override bool Equals(object other)
      {
         if(ReferenceEquals(other, null))
            return false;
         if(ReferenceEquals(other, this))
            return true;
         if(other.GetType() != typeof(Blob))
            return false;

         return Equals((Blob)other);
      }

      /// <summary>
      /// Hash code calculation
      /// </summary>
      public override int GetHashCode()
      {
         return FullPath.GetHashCode() * Kind.GetHashCode();
      }

      /// <summary>
      /// Constructs a file blob by full ID
      /// </summary>
      public static implicit operator Blob(string fullPath)
      {
         return new Blob(fullPath, BlobItemKind.File);
      }

      /// <summary>
      /// Converts blob to string by using full path
      /// </summary>
      /// <param name="blob"></param>
      public static implicit operator string(Blob blob)
      {
         return blob.FullPath;
      }

      /// <summary>
      /// Converts blob attributes (user metadata to byte array)
      /// </summary>
      /// <returns></returns>
      public byte[] AttributesToByteArray()
      {
         using(var ms = new MemoryStream())
         {
            using(var b = new BinaryWriter(ms, Encoding.UTF8, true))
            {
               b.Write((byte)1); //version marker

               b.Write((int)Metadata?.Count);   //number of metadata items

               foreach(KeyValuePair<string, string> pair in Metadata)
               {
                  b.Write(pair.Key);
                  b.Write(pair.Value);
               }
            }

            return ms.ToArray();
         }
      }

      /// <summary>
      /// Appends attributes from byte array representation
      /// </summary>
      /// <param name="data"></param>
      public void AppendAttributesFromByteArray(byte[] data)
      {
         if(data == null)
            return;

         using(var ms = new MemoryStream(data))
         {
            using(var b = new BinaryReader(ms, Encoding.UTF8, true))
            {
               byte version = b.ReadByte();  //to be used with versioning
               if(version != 1)
               {
                  throw new ArgumentException($"version {version} is not supported", nameof(data));
               }

               int count = b.ReadInt32();
               if(count > 0)
               {
                  for(int i = 0; i < count; i++)
                  {
                     string key = b.ReadString();
                     string value = b.ReadString();

                     Metadata[key] = value;
                  }
               }
            }
         }
      }

      /// <summary>
      /// Prepends path to this blob's path without modifying blob's properties
      /// </summary>
      /// <param name="path"></param>
      public void PrependPath(string path)
      {
         if(path == null || StoragePath.IsRootPath(path))
            return;

         FolderPath = StoragePath.Combine(path, FolderPath);
      }

      /// <summary>
      /// Changes full path of this blob without modifying any other property
      /// </summary>
      public void SetFullPath(string fullPath)
      {
         string path = StoragePath.Normalize(fullPath);

         if(StoragePath.IsRootPath(path))
         {
            Name = StoragePath.RootFolderPath;
            FolderPath = StoragePath.RootFolderPath;
         }
         else
         {
            string[] parts = StoragePath.Split(path);

            Name = parts.Last();
            FolderPath = StoragePath.GetParent(path);
         }
      }

      /// <summary>
      /// Clones blob to best efforts
      /// </summary>
      /// <returns></returns>
      public object Clone()
      {
         var clone = (Blob)MemberwiseClone();
         clone.Metadata = new Dictionary<string, string>(Metadata, StringComparer.OrdinalIgnoreCase);
         clone.Properties = new Dictionary<string, object>(Properties, StringComparer.OrdinalIgnoreCase);
         return clone;
      }
   }
}
