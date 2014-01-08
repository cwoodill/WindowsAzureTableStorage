using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WindowsAzureTableStorageUnitTests;

namespace WindowsAzureTableStorageImporter
{
    class WindowsAzureTableStorageImporter
    {
        static void Main(string[] args)
        {
            WindowsAzureTableStorageUnitTest test = new WindowsAzureTableStorageUnitTest();
            test.TestCreateBatches530Records();
        }
    }
}
