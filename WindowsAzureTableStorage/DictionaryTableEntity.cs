using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage;
using System.Collections;

namespace WindowsAzureTableStorage
{
    /// <summary>
    /// Dynamic Table Entity Structure that allows for any object to be used to insert into Table Storage as an Entity
    /// Partially borrowed from https://github.com/sandrinodimattia/WindowsAzure-DictionaryTableEntity and then revised to conform to ITableEntity interface.
    /// </summary>
    public class DictionaryTableEntity : ITableEntity, IDictionary<string, EntityProperty>
    {
        private IDictionary<string, EntityProperty> _properties;

        
        public DictionaryTableEntity()
        {
            _properties = new Dictionary<string, EntityProperty>();
        }

        public void Add(string key, EntityProperty value)
        {
            _properties.Add(key, value);
        }

        public void Add(string key, bool value)
        {
            _properties.Add(key, new EntityProperty(value));
        }


        public void Add(string key, byte[] value)
        {
            _properties.Add(key, new EntityProperty(value));
        }


        public void Add(string key, DateTime? value)
        {
            _properties.Add(key, new EntityProperty(value));
        }


        public void Add(string key, DateTimeOffset? value)
        {
            _properties.Add(key, new EntityProperty(value));
        }


        public void Add(string key, double value)
        {
            _properties.Add(key, new EntityProperty(value));
        }


        public void Add(string key, Guid value)
        {
            _properties.Add(key, new EntityProperty(value));
        }


        public void Add(string key, int value)
        {
            _properties.Add(key, new EntityProperty(value));
        }


        public void Add(string key, long value)
        {
            _properties.Add(key, new EntityProperty(value));
        }


        public void Add(string key, string value)
        {
            _properties.Add(key, new EntityProperty(value));
        }


        public bool ContainsKey(string key)
        {
            return _properties.ContainsKey(key);
        }


        public ICollection<string> Keys
        {
            get { return _properties.Keys; }
        }


        public bool Remove(string key)
        {
            return _properties.Remove(key);
        }


        public bool TryGetValue(string key, out EntityProperty value)
        {
            return _properties.TryGetValue(key, out value);
        }


        public ICollection<EntityProperty> Values
        {
            get { return _properties.Values; }
        }


        public EntityProperty this[string key]
        {
            get { return _properties[key]; }
            set { _properties[key] = value; }
        }


        public void Add(KeyValuePair<string, EntityProperty> item)
        {
            _properties.Add(item);
        }


        public void Clear()
        {
            _properties.Clear();
        }


        public bool Contains(KeyValuePair<string, EntityProperty> item)
        {
            return _properties.Contains(item);
        }


        public void CopyTo(KeyValuePair<string, EntityProperty>[] array, int arrayIndex)
        {
            _properties.CopyTo(array, arrayIndex);
        }


        public int Count
        {
            get { return _properties.Count; }
        }


        public bool IsReadOnly
        {
            get { return _properties.IsReadOnly; }
        }


        public bool Remove(KeyValuePair<string, EntityProperty> item)
        {
            return _properties.Remove(item);
        }


        public IEnumerator<KeyValuePair<string, EntityProperty>> GetEnumerator()
        {
            return _properties.GetEnumerator();
        }


        IEnumerator IEnumerable.GetEnumerator()
        {
            return _properties.GetEnumerator();
        }

        public string ETag
        {
            get;
            set;
        }

        public string PartitionKey
        {
            get;
            set;
        }

        public string RowKey
        {
            get;
            set;
        }

        public DateTimeOffset Timestamp
        {
            get;
            set;
        }


        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            this._properties = properties;
        }


        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            return _properties;
        }
    }
}