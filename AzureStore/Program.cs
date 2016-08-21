using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Concurrent;
using Common;

namespace AzureTable
{
    static class Program
    {
        private static CloudTable Table;
        private static CloudStorageAccount CloudStorageAccount;

        public static void Main(string[] args)
        {
            CloudStorageAccount CloudStorageAccount = CloudStorageAccount.Parse(ConfigurationSettings.AppSettings["StorageConnectionString"]);
            CloudTableClient tableClient = CloudStorageAccount.CreateCloudTableClient();
            Table = tableClient.GetTableReference(ConfigurationSettings.AppSettings["TableName"]);

            // Private method calls

            // TransformEntitiesAsync(c => c.PartitionKey.Equals("execution"), c => { var d = c; d["Shantanu"] = new EntityProperty("Singh"); return d; }).Wait();


            DeleteRecreateAsync(
                c => c.PartitionKey.Equals("execution_generalpool"), 
                c => 
                {
                    var d = c;
                    d.PartitionKey = "execution";
                    d.RowKey = c["JobId"].StringValue;
                    d.Properties.Remove("JobId");
                    return d;
                }).Wait();


            // End Private method calls

            Utilities.PrintAllErrors();
            Console.ReadKey();
        }

        private static async Task DeleteRecreateAsync(Func<DynamicTableEntity, bool> filterFunc, Func<DynamicTableEntity, DynamicTableEntity> transformFunc, bool useParallel = true, bool deleteOld = false, bool recreate = true)
        {
            Utilities.PrintWithTime("Fetching entities..");
            var returnedEntities = Table.CreateQuery<DynamicTableEntity>().Where(filterFunc).ToList();
            Utilities.PrintWithTime("Entities Fetched = {0}".FormatArgsInvariant(returnedEntities.Count));

            if (recreate)
            {
                if (useParallel)
                {
                    Parallel.ForEach(returnedEntities, entity =>
                    {
                        DynamicTableEntity transformedEntity = transformFunc(entity);
                        TableOperation insertOperation = TableOperation.InsertOrReplace(transformedEntity);
                        Utilities.PrintWithTime("InsertOrReplace at PK={0}, RK={1} - Started".FormatArgsInvariant(transformedEntity.PartitionKey, transformedEntity.RowKey));
                        try
                        {
                            Table.ExecuteAsync(insertOperation).Wait();
                            Utilities.PrintWithTime("InsertOrReplace at PK={0}, RK={1} - Completed".FormatArgsInvariant(transformedEntity.PartitionKey, transformedEntity.RowKey));
                        }
                        catch (Exception e)
                        {
                            Utilities.PrintErrorWithTime("InsertOrReplace at PK={0}, RK={1} - Failed. Exception={2}".FormatArgsInvariant(transformedEntity.PartitionKey, transformedEntity.RowKey, e.Message));
                        }
                    });
                }
                else
                {
                    foreach (var entity in returnedEntities)
                    {
                        DynamicTableEntity transformedEntity = transformFunc(entity);
                        TableOperation insertOperation = TableOperation.InsertOrReplace(transformedEntity);
                        Utilities.PrintWithTime("InsertOrReplace at PK={0}, RK={1} - Started".FormatArgsInvariant(transformedEntity.PartitionKey, transformedEntity.RowKey));
                        try
                        {
                            await Table.ExecuteAsync(insertOperation);
                            Utilities.PrintWithTime("InsertOrReplace at PK={0}, RK={1} - Completed".FormatArgsInvariant(transformedEntity.PartitionKey, transformedEntity.RowKey));
                        }
                        catch (Exception e)
                        {
                            Utilities.PrintErrorWithTime("InsertOrReplace at PK={0}, RK={1} - Failed. Exception={2}".FormatArgsInvariant(transformedEntity.PartitionKey, transformedEntity.RowKey, e.Message));
                        }
                    }
                }
            }

            if (deleteOld)
            {
                if (useParallel)
                {
                    Parallel.ForEach(returnedEntities, entity =>
                    {
                        TableOperation deleteOperation = TableOperation.Delete(entity);
                        Utilities.PrintWithTime("Delete at PK={0}, RK={1} - Started".FormatArgsInvariant(entity.PartitionKey, entity.RowKey));
                        try
                        {
                            Table.ExecuteAsync(deleteOperation).Wait();
                            Utilities.PrintWithTime("Delete at PK={0}, RK={1} - Completed".FormatArgsInvariant(entity.PartitionKey, entity.RowKey));
                        }
                        catch (Exception e)
                        {
                            Utilities.PrintErrorWithTime("Delete at PK={0}, RK={1} - Failed. Exception={2}".FormatArgsInvariant(entity.PartitionKey, entity.RowKey, e.Message));
                        }
                    });
                }
                else
                {
                    foreach (var entity in returnedEntities)
                    {
                        TableOperation deleteOperation = TableOperation.Delete(entity);
                        Utilities.PrintWithTime("Delete at PK={0}, RK={1} - Started".FormatArgsInvariant(entity.PartitionKey, entity.RowKey));
                        try
                        {
                            await Table.ExecuteAsync(deleteOperation);
                            Utilities.PrintWithTime("Delete at PK={0}, RK={1} - Completed".FormatArgsInvariant(entity.PartitionKey, entity.RowKey));
                        }
                        catch (Exception e)
                        {
                            Utilities.PrintErrorWithTime("Delete at PK={0}, RK={1} - Failed. Exception={2}".FormatArgsInvariant(entity.PartitionKey, entity.RowKey, e.Message));
                        }

                    }
                }
            }
        }

        private static async Task TransformEntitiesAsync(Func<DynamicTableEntity, bool> filterFunc, Func<DynamicTableEntity, DynamicTableEntity> transformFunc)
        {
            Utilities.PrintWithTime("Fetching entities..");
            var returnedEntities = Table.CreateQuery<DynamicTableEntity>().Where(filterFunc).ToList();
            Utilities.PrintWithTime("Entities Fetched = {0}".FormatArgsInvariant(returnedEntities.Count));

            TableBatchOperation batchOperation = new TableBatchOperation();
            int counter = 0;

            foreach (var entity in returnedEntities)
            {
                counter++;

                DynamicTableEntity transformedEntity = transformFunc(entity);
                batchOperation.InsertOrReplace(transformedEntity);
                Utilities.PrintWithTime("InsertOrReplace at PK={0}, RK={1} - Included in batch".FormatArgsInvariant(transformedEntity.PartitionKey, transformedEntity.RowKey));

                // batch currently supports max 100 transactions
                if(counter == 99)
                {
                    counter = 0;
                    try
                    {
                        Utilities.PrintWithTime("Batch Commit - Started");
                        await Table.ExecuteBatchAsync(batchOperation);
                        Utilities.PrintWithTime("Batch Commit- Completed");
                    }
                    catch (Exception e)
                    {
                        Utilities.PrintErrorWithTime("Batch Commit - Failed. Exception={0}".FormatArgsInvariant(e.Message));
                    }
                    batchOperation = new TableBatchOperation();
                }
            }

            if(batchOperation.Count > 0)
            {
                try
                {
                    Utilities.PrintWithTime("Batch Commit - Started");
                    await Table.ExecuteBatchAsync(batchOperation);
                    Utilities.PrintWithTime("Batch Commit- Completed");
                }
                catch (Exception e)
                {
                    Utilities.PrintErrorWithTime("Batch Commit - Failed. Exception={0}".FormatArgsInvariant(e.Message));
                }
            }
        }
    }
}
