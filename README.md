WindowsAzureTableStorage
========================

Utility for Moving CSV files into Windows Azure Table Storage

Solution Description:

1. 	WindowsAzureTableStorage - Main Solution
1.1 	WindowsAzureTableStorage - Library for managing connections and transactions with WindowsAzureTableStorage
1.2 	WindowsAzureTableStorageImporter - Console application that reads in CSV file and moves the records as entities into WindowsAzureTableStorage
1.3 	WindowsAzureTableStorageUnitTests - Unit Tests for Library

App.Config Settings
StorageConnectionString		ConnectionString for WindowsAzureStorageAccount
FileName 			Path to CSV File
PartitionKeyField		Name of one of the fields in the CSV to use as a partitionkey
RowKeyField			Name of one of the fields to use as a rowkeyfield - if this is not supplied then importer will use Guid.NewGuid() to generate a new ID
AzureTableName			Name of the table to create and add the entities
MaximumRowsToImport		Maximum number of rows to load into Azure - if this is left out or less than 0 the program will load in all rows available.
StartingOffset			Starting row to start adding from the CSV file.
MaximumTasks			Maximum number of async requests to create before waiting for them to finish.