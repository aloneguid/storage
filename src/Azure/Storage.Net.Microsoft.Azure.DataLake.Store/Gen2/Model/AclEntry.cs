using System;
using System.Linq;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Model
{
   /// <summary>
   /// Access Control List entry
   /// </summary>
   public class AclEntry
   {
      /// <summary>
      /// Creates entry from it's text representation
      /// </summary>
      /// <param name="textForm">Text form i.e. 'rwx'</param>
      public AclEntry(string textForm)
      {
         if(textForm is null)
            throw new ArgumentNullException(nameof(textForm));

         string[] parts = textForm.Split(':');

         Type = parts[0];

         string objectId = parts[1];
         ObjectId = objectId.Length == 0 ? null : objectId;

         string rwx = parts[2];
         if(rwx.Length != 3)
            throw new ArgumentException($"must have exactly 3 characters, but value is [{textForm}]", nameof(textForm));

         CanRead = rwx[0] == 'r';
         CanWrite = rwx[1] == 'w';
         CanExecute = rwx[2] == 'x';
      }

      /// <summary>
      /// Creates a new access control entry
      /// </summary>
      /// <param name="objectType">Object type, should be "user" or "group"</param>
      /// <param name="objectId"></param>
      /// <param name="canRead"></param>
      /// <param name="canWrite"></param>
      /// <param name="canExecute"></param>
      public AclEntry(ObjectType objectType, string objectId, bool canRead, bool canWrite, bool canExecute)
      {
         Type = objectType.ToString().ToLower();   //todo: make it right
         ObjectId = objectId ?? throw new ArgumentNullException(nameof(objectId));
         CanRead = canRead;
         CanWrite = canWrite;
         CanExecute = canExecute;
      }

      //there supposed to be a scope field, but I didn't see one in the result

      /// <summary>
      /// Object type (user, group, etc.)
      /// </summary>
      public string Type { get; }

      /// <summary>
      /// Object ID. When not present contains null.
      /// </summary>
      public string ObjectId { get; }

      /// <summary>
      /// Read allowed ('r' flag)
      /// </summary>
      public bool CanRead { get; }

      /// <summary>
      /// Write allowed ('w' flag)
      /// </summary>
      public bool CanWrite { get; }

      /// <summary>
      /// Execute allowed ('x' flag)
      /// </summary>
      public bool CanExecute { get; }

      /// <summary>
      /// Converts to POSIX format
      /// </summary>
      /// <returns></returns>
      public override string ToString() => $"{Type}:{ObjectId}:{ToChar("r", CanRead)}{ToChar("w", CanWrite)}{ToChar("x", CanExecute)}";

      private static string ToChar(string name, bool value)
      {
         return value ? name : "-";
      }
   }
}
