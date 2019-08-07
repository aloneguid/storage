﻿using System.IO;
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

      #endregion

      #region [ Path ]

      /// <summary>
      /// https://docs.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/path/create
      /// </summary>
      /// <param name="path">The file or directory path.</param>
      /// <param name="resource">Required only for Create File and Create Directory. The value must be "file" or "directory".</param>
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
         [AliasAs("timeout")] int? timeoutSeconds = null,
         [Body] Stream body = null);

      #endregion
   }
}
