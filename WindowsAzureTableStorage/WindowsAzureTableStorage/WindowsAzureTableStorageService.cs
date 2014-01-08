using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

namespace WindowsAzureTableStorage
{
    /// <summary>
    /// Utility class for connecting to and storing data in Windows Azure Table Storage
    /// </summary>
    public class WindowsAzureTableStorageService
    {
        string storageConnectionString = "";
        CloudTableClient tableClient = null;
        private List<Task> batchTasks;

        /// <summary>
        /// Default constructor - no initialization of Azure is done before calling a specific method.
        /// </summary>
        /// <param name="StorageConnectionString">Connection string for azure table storage account</param>
        public WindowsAzureTableStorageService(string StorageConnectionString)
        {
            storageConnectionString = StorageConnectionString;
        }

        public void createCloudTableClient()
        {
            if (tableClient == null)
            {
                // Retrieve the storage account from the connection string.
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
                // Create the table client.
                tableClient = storageAccount.CreateCloudTableClient();
            }
        }

        public void closeCloudTableClient()
        {
            if (tableClient != null)
            {
                tableClient = null;
            }
        }

        /// <summary>
        /// Creates a table in Windows Azure Table Storage
        /// </summary>
        /// <param name="TableName">Name of the table to create</param>
        public void CreateTable(string TableName, Boolean DeleteIfExists)
        {
        
            // Create the table if it doesn't exist.
            CloudTable table = tableClient.GetTableReference(TableName);
            // Delete the table if deleteIfExists is true
            if (DeleteIfExists)
                table.DeleteIfExists();

            table = tableClient.GetTableReference(TableName);
            table.CreateIfNotExists();
            
        }

        public void AddEntity(String TableName, ITableEntity Entity )
        {
            // Retrieve the storage account from the connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference(TableName);

            // Create the TableOperation that inserts the customer entity.
            TableOperation insertOperation = TableOperation.Insert(Entity);

            // Execute the insert operation.
            table.Execute(insertOperation);
        }

        /// <summary>
        /// Adds batch of entities based on the following rules:
        /// 1. Batch maximum size is 100 records
        /// 2. Batch entitities must all share the same partition key
        /// Batches are put in through ASync tasks so that we can create as many HTTP calls as possible to improve performance.
        /// </summary>
        /// <param name="TableName">Name of the table to insert into</param>
        /// <param name="entities">List of entities to insert into the table</param>
        public async Task AddBatchAsync(String TableName, List<ITableEntity> entities )
        {
            batchTasks = new List<Task>();

            // Retrieve the storage account from the connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
                        
            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference(TableName);

            List<TableBatchOperation> batchOperations = new List<TableBatchOperation>();

            // run a lync query to group list of entities by Partition Key
            var queryPartitionKey = from entity in entities group entity by entity.PartitionKey into partitiongroup orderby partitiongroup.Key select partitiongroup; 

            foreach (var partitionGroup in queryPartitionKey)
            {
                string partitionKey = partitionGroup.Key;
                int currentBatchRecords = 0;
                TableBatchOperation batch = new TableBatchOperation();
                foreach (ITableEntity entity in partitionGroup)
                {
                    if (currentBatchRecords < 100)
                    {
                        // currently less than the maximum number of records for a batch, so we can add to the current batch.
                        batch.Insert(entity);
                        currentBatchRecords++;
                    }
                    else
                    {
                        batchOperations.Add(batch);
                        batch = new TableBatchOperation();
                        currentBatchRecords = 0;
                        batch.Insert(entity);
                        currentBatchRecords++;
                    }
                }
                batchOperations.Add(batch);
            }
            // now we have collected all the batches, execute batches asyncronously
            foreach (TableBatchOperation batch in batchOperations)
            {
                Task<IList<TableResult>> task = table.ExecuteBatchAsync(batch);
                batchTasks.Add(task);
            }
            await Task.WhenAll(batchTasks);
        }

        /// <summary>
        /// Adds batch of entities based on the following rules:
        /// 1. Batch maximum size is 100 records
        /// 2. Batch entitities must all share the same partition key
        /// Batches are put through syncronously.
        /// </summary>
        /// <param name="TableName">Name of the table to insert into</param>
        /// <param name="entities">List of entities to insert into the table</param>
        public void AddBatch(String TableName, List<ITableEntity> entities)
        {
            // Retrieve the storage account from the connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference(TableName);

            List<TableBatchOperation> batchOperations = new List<TableBatchOperation>();

            // run a lync query to group list of entities by Partition Key
            var queryPartitionKey = from entity in entities group entity by entity.PartitionKey into partitiongroup orderby partitiongroup.Key select partitiongroup;

            foreach (var partitionGroup in queryPartitionKey)
            {
                string partitionKey = partitionGroup.Key;
                int currentBatchRecords = 0;
                TableBatchOperation batch = new TableBatchOperation();
                foreach (ITableEntity entity in partitionGroup)
                {
                    if (currentBatchRecords < 100)
                    {
                        // currently less than the maximum number of records for a batch, so we can add to the current batch.
                        batch.Insert(entity);
                        currentBatchRecords++;
                    }
                    else
                    {
                        batchOperations.Add(batch);
                        batch = new TableBatchOperation();
                        currentBatchRecords = 0;
                        batch.Insert(entity);
                        currentBatchRecords++;
                    }
                }
                batchOperations.Add(batch);
            }
            // now we have collected all the batches, execute batches asyncronously
            foreach (TableBatchOperation batch in batchOperations)
            {
                table.ExecuteBatch(batch);
            }
        }

    }
}
