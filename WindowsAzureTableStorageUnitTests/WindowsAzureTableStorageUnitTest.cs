using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Configuration;
using System.Collections.Specialized;
using WindowsAzureTableStorage;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Table;

namespace WindowsAzureTableStorageUnitTests
{
    [TestClass]
    public class WindowsAzureTableStorageUnitTest
    {
        [TestMethod]
        public void TestConnectToAzure()
        {
            // retrieve the connection string
            NameValueCollection appsettings = ConfigurationManager.AppSettings;
            string storageConnectionString = appsettings["StorageConnectionString"];
            WindowsAzureTableStorageService dao = new WindowsAzureTableStorageService(storageConnectionString);
            dao.createCloudTableClient();
        }

        [TestMethod]
        public void TestCreateEntity()
        {
            // retrieve the connection string
            NameValueCollection appsettings = ConfigurationManager.AppSettings;
            string storageConnectionString = appsettings["StorageConnectionString"];
            WindowsAzureTableStorageService dao = new WindowsAzureTableStorageService(storageConnectionString);
            dao.createCloudTableClient();
            dao.CreateTable("test");
            DictionaryTableEntity entity = new DictionaryTableEntity();
            entity.PartitionKey = "Test";
            entity.RowKey = Guid.NewGuid().ToString();
            entity.Add("city", "seattle");
            entity.Add("street", "111 South Jackson");
            dao.AddEntity("test", entity);
        }

        [TestMethod]
        public void TestCreateBatchesLessThan100Records()
        {
            // retrieve the connection string
            NameValueCollection appsettings = ConfigurationManager.AppSettings;
            string storageConnectionString = appsettings["StorageConnectionString"];
            WindowsAzureTableStorageService dao = new WindowsAzureTableStorageService(storageConnectionString);
            dao.createCloudTableClient();
            dao.CreateTable("test");
            List<ITableEntity> entities = new List<ITableEntity>();

            for (int i = 0; i < 50; i++)
            {
                DictionaryTableEntity entity = new DictionaryTableEntity();
                entity.PartitionKey = "Test";
                entity.RowKey = Guid.NewGuid().ToString();
                entity.Add("city", "seattle");
                entity.Add("street", "111 South Jackson");
                entities.Add(entity);
            }
            dao.AddBatch("test", entities);
        }

        [TestMethod]
        public void TestCreateBatches100Records()
        {
            // retrieve the connection string
            NameValueCollection appsettings = ConfigurationManager.AppSettings;
            string storageConnectionString = appsettings["StorageConnectionString"];
            WindowsAzureTableStorageService dao = new WindowsAzureTableStorageService(storageConnectionString);
            dao.createCloudTableClient();
            dao.CreateTable("test");
            List<ITableEntity> entities = new List<ITableEntity>();

            for (int i = 0; i < 100; i++)
            {
                DictionaryTableEntity entity = new DictionaryTableEntity();
                entity.PartitionKey = "Test";
                entity.RowKey = Guid.NewGuid().ToString();
                entity.Add("city", "seattle");
                entity.Add("street", "111 South Jackson");
                entities.Add(entity);
            }
            dao.AddBatch("test", entities);
        }


        [TestMethod]
        public void TestCreateBatches150Records()
        {
            // retrieve the connection string
            NameValueCollection appsettings = ConfigurationManager.AppSettings;
            string storageConnectionString = appsettings["StorageConnectionString"];
            WindowsAzureTableStorageService dao = new WindowsAzureTableStorageService(storageConnectionString);
            dao.createCloudTableClient();
            dao.CreateTable("test");
            List<ITableEntity> entities = new List<ITableEntity>();

            for (int i = 0; i < 150; i++)
            {
                DictionaryTableEntity entity = new DictionaryTableEntity();
                entity.PartitionKey = "Test";
                entity.RowKey = Guid.NewGuid().ToString();
                entity.Add("city", "seattle");
                entity.Add("street", "111 South Jackson");
                entities.Add(entity);
            }
            dao.AddBatch("test", entities);
        }


        [TestMethod]
        public void TestCreateBatches500Records()
        {
            // retrieve the connection string
            NameValueCollection appsettings = ConfigurationManager.AppSettings;
            string storageConnectionString = appsettings["StorageConnectionString"];
            WindowsAzureTableStorageService dao = new WindowsAzureTableStorageService(storageConnectionString);
            dao.createCloudTableClient();
            dao.CreateTable("test");
            List<ITableEntity> entities = new List<ITableEntity>();

            for (int i = 0; i < 500; i++)
            {
                DictionaryTableEntity entity = new DictionaryTableEntity();
                entity.PartitionKey = "Test";
                entity.RowKey = Guid.NewGuid().ToString();
                entity.Add("city", "seattle");
                entity.Add("street", "111 South Jackson");
                entities.Add(entity);
            }
            dao.AddBatch("test", entities);
        }

        [TestMethod]
        public void TestCreateBatches530Records()
        {
            // retrieve the connection string
            NameValueCollection appsettings = ConfigurationManager.AppSettings;
            string storageConnectionString = appsettings["StorageConnectionString"];
            WindowsAzureTableStorageService dao = new WindowsAzureTableStorageService(storageConnectionString);
            dao.createCloudTableClient();
            dao.CreateTable("test");
            List<ITableEntity> entities = new List<ITableEntity>();

            for (int i = 0; i < 530; i++)
            {
                DictionaryTableEntity entity = new DictionaryTableEntity();
                entity.PartitionKey = "Test";
                entity.RowKey = Guid.NewGuid().ToString();
                entity.Add("city", "seattle");
                entity.Add("street", "111 South Jackson");
                entities.Add(entity);
            }
            var task = dao.AddBatchAsync("test", entities);
            task.Wait();
        }

        [TestMethod]
        public void TestCreateBatchesWithMixedPartitionKeys()
        {
            // retrieve the connection string
            NameValueCollection appsettings = ConfigurationManager.AppSettings;
            string storageConnectionString = appsettings["StorageConnectionString"];
            WindowsAzureTableStorageService dao = new WindowsAzureTableStorageService(storageConnectionString);
            dao.createCloudTableClient();
            dao.CreateTable("test");
            List<ITableEntity> entities = new List<ITableEntity>();

            DictionaryTableEntity entity = new DictionaryTableEntity();
            entity.PartitionKey = "PartitionKey1";
            entity.RowKey = Guid.NewGuid().ToString();
            entity.Add("city", "seattle");
            entity.Add("street", "111 South Jackson");
            entities.Add(entity);

            entity = new DictionaryTableEntity();
            entity.PartitionKey = "PartitionKey2";
            entity.RowKey = Guid.NewGuid().ToString();
            entity.Add("city", "seattle");
            entity.Add("street", "111 South Jackson");
            entities.Add(entity);

            entity = new DictionaryTableEntity();
            entity.PartitionKey = "PartitionKey1";
            entity.RowKey = Guid.NewGuid().ToString();
            entity.Add("city", "seattle");
            entity.Add("street", "111 South Jackson");
            entities.Add(entity);

            entity = new DictionaryTableEntity();
            entity.PartitionKey = "PartitionKey2";
            entity.RowKey = Guid.NewGuid().ToString();
            entity.Add("city", "seattle");
            entity.Add("street", "111 South Jackson");
            entities.Add(entity);

            dao.AddBatch("test", entities);
        }

    }

}
