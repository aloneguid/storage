using System;
using System.Linq;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs.Gen2.Model
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
         Identity = objectId.Length == 0 ? null : objectId;

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
      /// <param name="identity"></param>
      /// <param name="canRead"></param>
      /// <param name="canWrite"></param>
      /// <param name="canExecute"></param>
      public AclEntry(ObjectType objectType, string identity, bool canRead, bool canWrite, bool canExecute)
      {
         Type = objectType.ToString().ToLower();   //todo: make it right
         Identity = identity ?? throw new ArgumentNullException(nameof(identity));
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
      /// Object ID or UPN when present.
      /// </summary>
      public string Identity { get; }

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
      public override string ToString() => $"{Type}:{Identity}:{ToChar("r", CanRead)}{ToChar("w", CanWrite)}{ToChar("x", CanExecute)}";

      private static string ToChar(string name, bool value)
      {
         return value ? name : "-";
      }
   }
}
