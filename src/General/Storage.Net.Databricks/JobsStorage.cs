using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Databricks.Client;
using Newtonsoft.Json;
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

            // convert in parallel as they need to fetch extra info per item
            return await Task.WhenAll(jobs.Select(j => ToBlobAsync(j))).ConfigureAwait(false);
         }

         // list runs
         // need job ID here - find by job name (crap!)
         string jobName = StoragePath.Split(path)[0];
         long? jobId = await GetJobIdFromJobNameAsync(jobName);
         IReadOnlyCollection<Run> allRuns = await GetAllJobRunsAsync(jobId.Value);
         List<Blob> rr = allRuns.Select(r => ToBlob(jobName, r)).ToList();
         rr.Reverse();
         return rr;
      }

      private async Task<IReadOnlyCollection<Run>> GetAllJobRunsAsync(long jobId)
      {
         var result = new List<Run>();
         RunList runsList;
         int offset = 0;
         do
         {
            runsList = await _jobs.RunsList(jobId, offset, 500, false, false).ConfigureAwait(false);
            List<Run> runs = runsList.Runs.ToList();
            offset += runs.Count;
            result.AddRange(runs);
         }
         while(runsList.HasMore);

         return result;
      }

      public override async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         // download run: path is [job name]/[run id]

         string[] parts = StoragePath.Split(fullPath);

         if(parts.Length == 2)
         {
            if(long.TryParse(parts[1], out long runId))
            {
               IEnumerable<ViewItem> exportedViews = await _jobs.RunsExport(runId, ViewsToExport.ALL);
               if(exportedViews == null)
                  return null;

               ViewItem exportedView = exportedViews.FirstOrDefault();

               return exportedView?.Content == null
                  ? null
                  : new MemoryStream(Encoding.UTF8.GetBytes(exportedView.Content));
            }
         }

         return null;
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

      private async Task<Blob> ToBlobAsync(Job dbJob)
      {
         var blob = new Blob(dbJob.Settings.Name, BlobItemKind.Folder);
         blob.CreatedTime = blob.LastModificationTime = dbJob.CreatedTime;
         blob.TryAddProperties(
            "ObjectType", "job",
            "Id", dbJob.JobId,
            "CreatorUserName", dbJob.CreatorUserName,
            "QuartzCronExpression", dbJob.Settings?.Schedule?.QuartzCronExpression,
            "MaxRetries", dbJob.Settings.MaxRetries,
            "MinRetryInterval", TimeSpan.FromMilliseconds(dbJob.Settings.MinRetryIntervalMilliSeconds),
            "RetryOnTimeout", dbJob.Settings.RetryOnTimeout,
            "MaxConcurrentRuns", dbJob.Settings.MaxConcurrentRuns,
            "Timeout", TimeSpan.FromSeconds(dbJob.Settings.TimeoutSeconds),
            "Settings", dbJob.Settings,
            "SettingJson", Module.AsDbJson(dbJob.Settings),
            "EmailNotifications", dbJob.Settings?.EmailNotifications,
            "EmailNotificationsJson", Module.AsDbJson(dbJob.Settings?.EmailNotifications));

         // get last run
         Run lastRun = (await _jobs.RunsList(dbJob.JobId, 0, 1)).Runs.FirstOrDefault();
         if(lastRun != null)
         {
            AddProperties(blob, lastRun, "LastRun");
         }

         return blob;
      }

      private static Blob ToBlob(string jobName, Run dbRun)
       {
         var blob = new Blob(jobName, dbRun.RunId.ToString(), BlobItemKind.File);
         blob.LastModificationTime = dbRun.EndTime ?? dbRun.StartTime;
         AddProperties(blob, dbRun);
         return blob;
      }

      private static void AddProperties(Blob blob, Run dbRun, string prefix = null)
      {
         blob.TryAddPropertiesWithPrefix(prefix,
            "Creator", dbRun.CreatorUserName,
            "JobId", dbRun.JobId,
            "OriginalAttemptRunId", dbRun.OriginalAttemptRunId,
            "LifeCycleState", dbRun.State.LifeCycleState,
            "ResultState", dbRun.State.ResultState,
            "StateMessage", dbRun.State.StateMessage,
            "QuartzCronExpression", dbRun.Schedule?.QuartzCronExpression,
            "StartTime", dbRun.StartTime,
            "EndTime", dbRun.EndTime,
            "SetupDuration", TimeSpan.FromMilliseconds(dbRun.SetupDuration),
            "ExecutionDuration", TimeSpan.FromMilliseconds(dbRun.ExecutionDuration),
            "CleanupDuration", TimeSpan.FromMilliseconds(dbRun.CleanupDuration),
            "Trigger", dbRun.Trigger,
            "RunPageUrl", dbRun.RunPageUrl,
            "IsCompleted", dbRun.IsCompleted,
            "RunId", dbRun.RunId,
            "NumberInJob", dbRun.NumberInJob,
            "Task", dbRun.Task,
            "TaskJson", Module.AsDbJson(dbRun.Task),
            "ClusterSpec", dbRun.ClusterSpec,
            "ClusterSpecJson", Module.AsDbJson(dbRun.ClusterSpec),
            "ClusterInstance", dbRun.ClusterInstance,
            "ClusterInstanceJson", Module.AsDbJson(dbRun.ClusterInstance),
            "OverridingParameters", dbRun.OverridingParameters,
            "OverridingParametersJson", Module.AsDbJson(dbRun.OverridingParameters));
      }
   }
}
