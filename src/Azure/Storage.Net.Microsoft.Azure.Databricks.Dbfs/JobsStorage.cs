using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Databricks.Client;
using Storage.Net.Blobs;

namespace Storage.Net.Databricks
{
   class JobsStorage : GenericBlobStorage
   {
      private readonly IJobsApi _jobs;

      public JobsStorage(IJobsApi jobs) => _jobs = jobs;

      protected override bool CanListHierarchy => false;


      protected override async Task<IReadOnlyCollection<Blob>> ListAtAsync(
         string path, ListOptions options, CancellationToken cancellationToken)
      {
         if(StoragePath.IsRootPath(path))
         {
            IEnumerable<Job> jobs = await _jobs.List();
            return jobs.Select(ToBlob).ToList();
         }

         return null;
      }

      private static Blob ToBlob(Job dbJob)
      {
         var blob = new Blob(dbJob.Settings.Name, BlobItemKind.Folder);
         blob.LastModificationTime = dbJob.CreatedTime;
         blob.TryAddProperties(
            "ObjectType", "job",
            "Id", dbJob.JobId,
            "CreatorUserName", dbJob.CreatorUserName,
            "Settings", dbJob.Settings);

         return blob;
      }
   }
}
