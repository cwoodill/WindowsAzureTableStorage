using CsvHelper;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Specialized;
using WindowsAzureTableStorage;
using System.Diagnostics;


namespace WindowsAzureTableStorageImporter
{
    class WindowsAzureTableStorageImporter
    {
        static void Main(string[] args)
        {
            try
            {
                // load configuration settings from the app.config

                // output debug information to the console
                TextWriterTraceListener myWriter = new
                   TextWriterTraceListener(System.Console.Out);
                Debug.Listeners.Add(myWriter);

                NameValueCollection appsettings = ConfigurationManager.AppSettings;
                string fileName = appsettings["FileName"];            // get configuration data from App.Config
                string partitionKeyField = appsettings["PartitionKeyField"];
                string rowKeyField = appsettings["RowKeyField"];
                string azureTableName = appsettings["AzureTableName"];
                string storageConnectionString = appsettings["StorageConnectionString"];
                string maximumRowsToImportString = appsettings["MaximumRowsToImport"];
                string maximumTasksString = appsettings["MaximumTasks"];
                string startingOffsetString = appsettings["startingOffset"];

                int maximumRowsToImport = 0;
                if (maximumRowsToImportString != null)
                {
                    maximumRowsToImport = int.Parse(maximumRowsToImportString);
                }

                int maximumTasks = 0;
                if (maximumTasks != null)
                {
                    maximumTasks = int.Parse(maximumTasksString);
                }

                int startingOffset = 0;
                if (startingOffsetString != null)
                {
                    startingOffset = int.Parse(startingOffsetString);
                }

                // create storage service and list of entities to populate
                List<ITableEntity> entities = new List<ITableEntity>();
                WindowsAzureTableStorageService tableStorageService = new WindowsAzureTableStorageService(storageConnectionString);
                tableStorageService.CreateTable(azureTableName);

                // open the file and start reading
                StreamReader textReader = File.OpenText(fileName);
                CsvReader reader = new CsvReader(textReader);
                
                while (reader.Read())
                {
                    if (reader.Row-1 < startingOffset)
                    {
                        // Row -1 is because the header
                        // do nothing - move read to the startingOffset
                    }
                    else
                    {
                        // populate an entity for each row
                        DictionaryTableEntity entity = new DictionaryTableEntity();

                        if (rowKeyField == null)
                        {
                            entity.RowKey = Guid.NewGuid().ToString();
                        }

                        foreach (string field in reader.FieldHeaders)
                        {
                            if (field == rowKeyField)
                            {
                                entity.RowKey = reader[field];
                            }
                            else if (field == partitionKeyField)
                            {
                                entity.PartitionKey = WindowsAzureTableStorageService.createValidPartitionKey(reader[field]);
                            }
                            else
                            {
                                string value = reader[field];
                                entity.Add(field, value);
                            }
                        }
                        if (entity.PartitionKey == null)
                            throw new Exception("Bad data record. Partition key not found.");

                        entities.Add(entity);
                        if (maximumRowsToImport > 0 && entities.Count == maximumRowsToImport)
                            break;
                    }
                }
                Console.Out.WriteLine("Starting Upload of " + entities.Count + " entities to Windows Azure Table Storage at " + DateTime.Now);
                var task = tableStorageService.AddBatchAsync(azureTableName, entities, maximumTasks);
                task.Wait();
                Console.Out.WriteLine("Finished Upload to Windows Azure Table Storage at " + DateTime.Now);
            }
            catch (Exception e)
            {
                Console.Write(e.Message);
            }
        }
    }
}
