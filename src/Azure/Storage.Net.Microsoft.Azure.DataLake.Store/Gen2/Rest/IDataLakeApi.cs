using System.IO;
using System.Threading.Tasks;
using Refit;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest.Model;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest
{
   /// <summary>
   /// Refit interface wrapping the calls.
   /// REST API documentation - https://docs.microsoft.com/en-us/rest/api/storageservices/data-lake-storage-gen2
   /// </summary>
   interface IDataLakeApi
   {
      #region [ Filesystem ]

      /// <summary>
      /// https://docs.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/filesystem/list
      /// </summary>
      /// <returns></returns>
      [Get("/?resource=account")]
      Task<FilesystemList> ListFilesystemsAsync(
         string prefix = null,
         string continuation = null,
         int? maxResults = null,
         [AliasAs("timeout")] int? timeoutSeconds = null);

      [Put("/{filesystem}?resource=filesystem")]
      Task CreateFilesystemAsync(
         string filesystem,
         [AliasAs("timeout")] int? timeoutSeconds = null);

      [Delete("/{filesystem}?resource=filesystem")]
      Task DeleteFilesystemAsync(
         string filesystem,
         [AliasAs("timeout")] int? timeoutSeconds = null);

      [Head("/{filesystem}?resource=filesystem")]
      Task<ApiResponse<string>> GetFilesystemProperties(
         string filesystem,
         [AliasAs("timeout")] int? timeoutSeconds = null);

      #endregion

      #region [ Path ]

      /// <summary>
      /// https://docs.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/path/create
      /// </summary>
      /// <param name="filesystem"></param>
      /// <param name="path">The file or directory path.</param>
      /// <param name="resource">Required only for Create File and Create Directory. The value must be "file" or "directory".</param>
      /// <param name="continuation"></param>
      /// <param name="mode"></param>
      /// <param name="timeout"></param>
      [Put("/{filesystem}/{**path}")]
      Task CreatePathAsync(
         string filesystem,
         string path,
         string resource,
         string continuation = null,
         string mode = null,
         int? timeout = null);

      /// <summary>
      /// https://docs.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/path/list
      /// </summary>
      /// <param name="filesystem">The filesystem identifier. The value must start and end with a letter or number and must contain only letters, numbers, and the dash (-) character. Consecutive dashes are not permitted. All letters must be lowercase. The value must have between 3 and 63 characters.</param>
      /// <param name="directory">Filters results to paths within the specified directory. An error occurs if the directory does not exist.</param>
      /// <param name="recursive">If "true", all paths are listed; otherwise, only paths at the root of the filesystem are listed. If "directory" is specified, the list will only include paths that share the same root.</param>
      /// <param name="continuation">The number of paths returned with each invocation is limited. If the number of paths to be returned exceeds this limit, a continuation token is returned in the response header x-ms-continuation. When a continuation token is returned in the response, it must be specified in a subsequent invocation of the list operation to continue listing the paths.</param>
      /// <param name="maxResults">An optional value that specifies the maximum number of items to return. If omitted or greater than 5,000, the response will include up to 5,000 items.</param>
      /// <param name="upn">Optional. Valid only when Hierarchical Namespace is enabled for the account. If "true", the user identity values returned in the owner and group fields of each list entry will be transformed from Azure Active Directory Object IDs to User Principal Names. If "false", the values will be returned as Azure Active Directory Object IDs. The default value is false. Note that group and application Object IDs are not translated because they do not have unique friendly names.</param>
      /// <param name="timeoutSeconds">An optional operation timeout value in seconds. The period begins when the request is received by the service. If the timeout value elapses before the operation completes, the operation fails.</param>
      /// <returns></returns>
      [Get("/{filesystem}?resource=filesystem")]
      Task<PathList> ListPathAsync(
         string filesystem,
         string directory = null,
         bool? recursive = true,
         string continuation = null,
         int? maxResults = null,
         bool? upn = null,
         [AliasAs("timeout")] int? timeoutSeconds = null);

      /// <summary>
      /// https://docs.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/path/update
      /// </summary>
      /// <param name="filesystem"></param>
      /// <param name="path"></param>
      /// <param name="action">The action must be "append" to upload data to be appended to a file, "flush" to flush previously uploaded data to a file, "setProperties" to set the properties of a file or directory, or "setAccessControl" to set the owner, group, permissions, or access control list for a file or directory. Note that Hierarchical Namespace must be enabled for the account in order to use access control. Also note that the Access Control List (ACL) includes permissions for the owner, owning group, and others, so the x-ms-permissions and x-ms-acl request headers are mutually exclusive.</param>
      /// <param name="position"></param>
      /// <param name="retainUncommittedData"></param>
      /// <param name="close"></param>
      /// <param name="properties">Optional. User-defined properties to be stored with the file or directory, in the format of a comma-separated list of name and value pairs "n1=v1, n2=v2, ...", where each value is a base64 encoded string. Note that the string may only contain ASCII characters in the ISO-8859-1 character set. Valid only for the setProperties operation. If the file or directory exists, any properties not included in the list will be removed. All properties are removed if the header is omitted. To merge new and existing properties, first get all existing properties and the current E-Tag, then make a conditional request with the E-Tag and include values for all properties.</param>
      /// <param name="owner">Optional and valid only for the setAccessControl operation. Sets the owner of the file or directory.</param>
      /// <param name="group">Optional and valid only for the setAccessControl operation. Sets the owning group of the file or directory.</param>
      /// <param name="permissions">Optional and only valid if Hierarchical Namespace is enabled for the account. Sets POSIX access permissions for the file owner, the file owning group, and others. Each class may be granted read, write, or execute permission. The sticky bit is also supported. Both symbolic (rwxrw-rw-) and 4-digit octal notation (e.g. 0766) are supported. Invalid in conjunction with x-ms-acl.</param>
      /// <param name="acl">Optional and valid only for the setAccessControl operation. Sets POSIX access control rights on files and directories. The value is a comma-separated list of access control entries that fully replaces the existing access control list (ACL). Each access control entry (ACE) consists of a scope, a type, a user or group identifier, and permissions in the format "[scope:][type]:[id]:[permissions]". The scope must be "default" to indicate the ACE belongs to the default ACL for a directory; otherwise scope is implicit and the ACE belongs to the access ACL. There are four ACE types: "user" grants rights to the owner or a named user, "group" grants rights to the owning group or a named group, "mask" restricts rights granted to named users and the members of groups, and "other" grants rights to all users not found in any of the other entries. The user or group identifier is omitted for entries of type "mask" and "other". The user or group identifier is also omitted for the owner and owning group. The permission field is a 3-character sequence where the first character is 'r' to grant read access, the second character is 'w' to grant write access, and the third character is 'x' to grant execute permission. If access is not granted, the '-' character is used to denote that the permission is denied. For example, the following ACL grants read, write, and execute rights to the file owner and john.doe@contoso, the read right to the owning group, and nothing to everyone else: "user::rwx,user:john.doe@contoso:rwx,group::r--,other::---,mask=rwx". Invalid in conjunction with x-ms-permissions.</param>
      /// <param name="timeoutSeconds"></param>
      /// <param name="body"></param>
      /// <returns></returns>
      [Patch("/{filesystem}/{**path}")]
      Task UpdatePathAsync(
         string filesystem,
         string path,
         string action,
         long? position = null,
         bool? retainUncommittedData = null,
         bool? close = null,
         [Header("x-ms-properties")] string properties = null,
         [Header("x-ms-owner")] string owner = null,
         [Header("x-ms-group")] string group = null,
         [Header("x-ms-permissions")] string permissions = null,
         [Header("x-ms-acl")] string acl = null,
         [AliasAs("timeout")] int? timeoutSeconds = null,
         [Body] Stream body = null);

      /// <summary>
      /// https://docs.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/path/read
      /// </summary>
      /// <param name="filesystem"></param>
      /// <param name="path"></param>
      /// <param name="range">The HTTP Range request header specifies one or more byte ranges of the resource to be retrieved.</param>
      /// <param name="body">Body must be present but must be empty</param>
      /// <param name="timeoutSeconds"></param>
      /// <returns></returns>
      [Get("/{filesystem}/{**path}")]
      Task<Stream> ReadPathAsync(
         string filesystem,
         string path,
         [Header("range")] string range,
         [Body] Stream body,
         [AliasAs("timeout")] int? timeoutSeconds = null);

      /// <summary>
      /// https://docs.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/path/delete
      /// </summary>
      /// <param name="filesystem"></param>
      /// <param name="path"></param>
      /// <param name="recursive"></param>
      /// <param name="continuation"></param>
      /// <param name="timeoutSeconds"></param>
      /// <returns></returns>
      [Delete("/{filesystem}/{**path}")]
      Task DeletePathAsync(
         string filesystem,
         string path,
         bool? recursive = null,
         string continuation = null,
         [AliasAs("timeout")] int? timeoutSeconds = null);

      /// <summary>
      /// https://docs.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/path/getproperties
      /// </summary>
      /// <param name="filesystem"></param>
      /// <param name="path"></param>
      /// <param name="action">Optional. If the value is "getStatus" only the system defined properties for the path are returned. If the value is "getAccessControl" the access control list is returned in the response headers (Hierarchical Namespace must be enabled for the account), otherwise the properties are returned.</param>
      /// <param name="upn">Optional. Valid only when Hierarchical Namespace is enabled for the account. If "true", the user identity values returned in the x-ms-owner, x-ms-group, and x-ms-acl response headers will be transformed from Azure Active Directory Object IDs to User Principal Names. If "false", the values will be returned as Azure Active Directory Object IDs. The default value is false. Note that group and application Object IDs are not translated because they do not have unique friendly names.</param>
      /// <param name="timeoutSeconds"></param>
      /// <returns></returns>
      [Head("/{filesystem}/{**path}")]
      Task<ApiResponse<string>> GetPathPropertiesAsync(
         string filesystem,
         string path,
         string action = null,
         bool? upn = null,
         [AliasAs("timeout")] int? timeoutSeconds = null);

      #endregion
   }
}
