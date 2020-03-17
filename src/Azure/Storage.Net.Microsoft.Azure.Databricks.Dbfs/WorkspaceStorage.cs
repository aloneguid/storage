using Microsoft.Azure.Databricks.Client;
using Storage.Net.Blobs;

namespace Storage.Net.Databricks
{
   class WorkspaceStorage : GenericBlobStorage
   {
      private readonly IWorkspaceApi _api;

      public WorkspaceStorage(IWorkspaceApi api)
      {
         _api = api;
      }

      protected override bool CanListHierarchy => false;
   }
}
