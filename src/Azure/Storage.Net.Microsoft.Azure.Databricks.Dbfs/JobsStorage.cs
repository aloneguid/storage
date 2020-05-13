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

         // need job ID here - find by job name (crap!)
         var rr = new List<Blob>();
         string jobName = StoragePath.Split(path)[0];
         long? jobId = await GetJobIdFromJobNameAsync(jobName);
         RunList runsList;
         int offset = 0;
         do
         {
            runsList = await _jobs.RunsList(jobId, offset, 500, false, false);
            List<Run> runs = runsList.Runs.ToList();
            offset += runs.Count;

            rr.AddRange(runs.Select(r => ToBlob(jobName, jobId.Value, r)));
         }
         while(runsList.HasMore);

         return rr;
      }

      private async Task<long?> GetJobIdFromJobNameAsync(string jobName)
      {
         IEnumerable<Job> allJobs = await _jobs.List();

         foreach(Job j in allJobs)
         {
            if(j.Settings.Name == jobName)
            {
               return j.JobId;
            }
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

      private static Blob ToBlob(string jobName, long jobId, Run dbRun)
      {
         var blob = new Blob(jobName, dbRun.RunId.ToString(), BlobItemKind.File);
         blob.TryAddProperties(
            "StartTime", dbRun.StartTime,
            "EndTime", dbRun.EndTime,
            "ExecutionDuration", dbRun.ExecutionDuration,
            "IsCompleted", dbRun.IsCompleted,
            "JobId", dbRun.JobId,
            "NumberInJob", dbRun.NumberInJob,
            "RunId", dbRun.RunId,
            "RunPageUrl", dbRun.RunPageUrl,
            "Schedule", dbRun.Schedule,
            "SetupDuration", dbRun.SetupDuration,
            "State", dbRun.State,
            "Task", dbRun.Task,
            "Trigger", dbRun.Trigger);
         return blob;
      }
   }
}
