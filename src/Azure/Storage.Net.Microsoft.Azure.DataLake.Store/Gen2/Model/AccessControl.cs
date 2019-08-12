using System;
using System.Collections.Generic;
using System.Text;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Model
{
   /// <summary>
   /// Access control description for Azure Data Lake Gen 2 objects
   /// </summary>
   public class AccessControl
   {
      /// <summary>
      /// Creates with raw response strings
      /// </summary>
      /// <param name="owner"></param>
      /// <param name="group"></param>
      /// <param name="permissions"></param>
      /// <param name="acl"></param>
      public AccessControl(string owner, string group, string permissions, string acl)
      {
         OwnerUserId = owner;
         OwnerGroupId = group;
         RawPermissions = permissions;
         RawAcl = acl;
      }

      /// <summary>
      /// Owner of the file or directory
      /// </summary>
      public string OwnerUserId { get; }

      /// <summary>
      /// Owner group of file or directory
      /// </summary>
      public string OwnerGroupId { get; }

      /// <summary>
      /// POSIX access permissions for the file owner, the file owning group, and others. Each class may be granted read, write, or execute permission. The sticky bit is also supported. Both symbolic (rwxrw-rw-) and 4-digit octal notation (e.g. 0766) are supported. Invalid in conjunction with x-ms-acl.
      /// </summary>
      public string RawPermissions { get; }

      /// <summary>
      /// POSIX access control rights on files and directories. The value is a comma-separated list of access control entries that fully replaces the existing access control list (ACL). Each access control entry (ACE) consists of a scope, a type, a user or group identifier, and permissions in the format "[scope:][type]:[id]:[permissions]". The scope must be "default" to indicate the ACE belongs to the default ACL for a directory; otherwise scope is implicit and the ACE belongs to the access ACL. There are four ACE types: "user" grants rights to the owner or a named user, "group" grants rights to the owning group or a named group, "mask" restricts rights granted to named users and the members of groups, and "other" grants rights to all users not found in any of the other entries. The user or group identifier is omitted for entries of type "mask" and "other". The user or group identifier is also omitted for the owner and owning group. The permission field is a 3-character sequence where the first character is 'r' to grant read access, the second character is 'w' to grant write access, and the third character is 'x' to grant execute permission. If access is not granted, the '-' character is used to denote that the permission is denied. For example, the following ACL grants read, write, and execute rights to the file owner and john.doe@contoso, the read right to the owning group, and nothing to everyone else: "user::rwx,user:john.doe@contoso:rwx,group::r--,other::---,mask=rwx". Invalid in conjunction with x-ms-permissions.
      /// </summary>
      public string RawAcl { get; }
   }
}
