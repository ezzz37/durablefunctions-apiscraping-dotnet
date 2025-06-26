using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Octokit;

namespace FanOutFanInCrawler
{
    public static class Orchestrator
    {
        // this needs to be configured when creating your Azure Function
        // with the portal:
        // https://docs.microsoft.com/en-us/azure/azure-functions/functions-how-to-use-azure-function-app-settings
        // command line:
        //
        private static Func<string> getToken = () => Environment.GetEnvironmentVariable("GitHubToken", EnvironmentVariableTarget.Process);
        private static GitHubClient github = new GitHubClient(new ProductHeaderValue("FanOutFanInCrawler")) { Credentials = new Credentials(getToken()) };
        private static CloudStorageAccount account = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("AzureWebJobsStorage", EnvironmentVariableTarget.Process));

        /// <summary>
        /// Trigger function that will use HTTP to start an Orchestrator function
        /// </summary>
        /// <param name="req"></param>
        /// <param name="starter"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        [FunctionName("Orchestrator_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient client,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await client.StartNewAsync("Orchestrator", null, "Nuget");

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return client.CreateCheckStatusResponse(req, instanceId);
        }

    public static async Task CleanOldRepositoryData(
        [ActivityTrigger] IDurableActivityContext context,
        ILogger log)
{
    // 1) Read input and apply default if invalid
    int daysToRetain = context.GetInput<int>();
    if (daysToRetain <= 0)
    {
        daysToRetain = 90;
        log.LogWarning("Invalid retention period supplied; defaulting to 90 days.");
    }
    log.LogInformation($"Starting cleanup: removing entries older than {daysToRetain} days.");

    // 2) Parse storage connection string
    string storageConn = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
    if (string.IsNullOrWhiteSpace(storageConn))
    {
        log.LogError("Environment variable 'AzureWebJobsStorage' is not set.");
        return;
    }

    if (!CloudStorageAccount.TryParse(storageConn, out CloudStorageAccount storageAccount))
    {
        log.LogError("Failed to parse Azure storage connection string.");
        return;
    }

    // 3) Get table reference
    var tableClient = storageAccount.CreateCloudTableClient();
    var table = tableClient.GetTableReference("Repositories");

    if (!await table.ExistsAsync())
    {
        log.LogWarning("Table 'Repositories' does not exist—nothing to clean.");
        return;
    }

    // 4) Compute cutoff date
    DateTimeOffset cutoff = DateTimeOffset.UtcNow.AddDays(-daysToRetain);

    // 5) Build a query that runs server-side
    string filter = TableQuery.GenerateFilterConditionForDate(
        "Timestamp",
        QueryComparisons.LessThan,
        cutoff);
    var query = new TableQuery<Repository>().Where(filter);

    // 6) Execute query in segments
    var toDelete = new List<Repository>();
    TableContinuationToken token = null;
    do
    {
        var segment = await table.ExecuteQuerySegmentedAsync(query, token);
        token = segment.ContinuationToken;
        toDelete.AddRange(segment.Results);
    } while (token != null);

    if (!toDelete.Any())
    {
        log.LogInformation("No repository entries older than cutoff date were found.");
        return;
    }

    log.LogInformation($"Found {toDelete.Count} entries to delete.");

    // 7) Delete in batches of up to 100
    int deletedCount = 0;
    foreach (var batch in toDelete.Chunk(100))
    {
        var batchOp = new TableBatchOperation();
        foreach (var entity in batch)
        {
            batchOp.Delete(entity);
        }

        try
        {
            await table.ExecuteBatchAsync(batchOp);
            deletedCount += batch.Count;
            log.LogInformation($"Deleted batch of {batch.Count} entries.");
        }
        catch (StorageException ex)
        {
            log.LogError($"Error deleting batch: {ex.Message}");
            // Optionally implement retry logic here
        }
    }

    log.LogInformation($"Cleanup finished. Total deleted: {deletedCount}");
}
            

        [FunctionName("Orchestrator")]
        public static async Task<string> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            // retrieves the organization name from the Orchestrator_HttpStart function
            var organizationName = context.GetInput<string>();
            // retrieves the list of repositories for an organization by invoking a separate Activity Function.
            var repositories = await context.CallActivityAsync<List<(long id, string name)>>("GetAllRepositoriesForOrganization", organizationName);

            // Creates an array of task to store the result of each functions
            var tasks = new Task<(long id, int openedIssues, string name)>[repositories.Count];
            for (int i = 0; i < repositories.Count; i++)
            {
                // Starting a `GetOpenedIssues` activity WITHOUT `async`
                // This will starts Activity Functions in parallel instead of sequentially.
                tasks[i] = context.CallActivityAsync<(long, int, string)>("GetOpenedIssues", (repositories[i]));
            }

            // Wait for all Activity Functions to complete execution
            await Task.WhenAll(tasks);

            // Retrieve the result of each Activity Function and return them in a list
            var openedIssues = tasks.Select(x => x.Result).ToList();

            // Send the list to an Activity Function to save them to Blob Storage.
            await context.CallActivityAsync("SaveRepositories", openedIssues);

            return context.InstanceId;
        }

        [FunctionName("GetAllRepositoriesForOrganization")]
        public static async Task<List<(long id, string name)>> GetAllRepositoriesForOrganization([ActivityTrigger] IDurableActivityContext context)
        {
            // retrieves the organization name from the Orchestrator function
            var organizationName = context.GetInput<string>();
            // invoke the API to retrieve the list of repositories of a specific organization
            var repositories = (await github.Repository.GetAllForOrg(organizationName)).Select(x => (x.Id, x.Name)).ToList();
            return repositories;
        }

        [FunctionName("GetOpenedIssues")]
        public static async Task<(long id, int openedIssues, string name)> GetOpenedIssues([ActivityTrigger] IDurableActivityContext context)
        {
            // retrieve a tuple of repositoryId and repository name from the Orchestrator function
            var parameters = context.GetInput<(long id, string name)>();

            // retrieves a list of issues from a specific repository
            var issues = (await github.Issue.GetAllForRepository(parameters.id)).ToList();

            // returns a tuple of the count of opened issues for a specific repository
            return (parameters.id, issues.Count(x => x.State == ItemState.Open), parameters.name);
        }

        [FunctionName("SaveRepositories")]
        public static async Task SaveRepositories([ActivityTrigger] IDurableActivityContext context)
        {
            // retrieves a tuple from the Orchestrator function
            var parameters = context.GetInput<List<(long id, int openedIssues, string name)>>();

            // create the client and table reference for Blob Storage
            var client = account.CreateCloudTableClient();
            var table = client.GetTableReference("Repositories");

            // create the table if it doesn't exist already.
            await table.CreateIfNotExistsAsync();

            // creates a batch of operation to be executed
            var batchOperation = new TableBatchOperation();
            foreach (var parameter in parameters)
            {
                // Creates an operation to add the repository to Table Storage
                batchOperation.Add(TableOperation.InsertOrMerge(new Repository(parameter.id)
                {
                    OpenedIssues = parameter.openedIssues,
                    RepositoryName = parameter.name
                }));
            }

            await table.ExecuteBatchAsync(batchOperation);
        }

        public class Repository : TableEntity
        {
            public Repository(long id)
            {
                PartitionKey = "Default";
                RowKey = id.ToString();
            }
            public int OpenedIssues { get; set; }
            public string RepositoryName { get; set; }
        }
    }
}
