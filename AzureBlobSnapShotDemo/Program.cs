using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace AzureBlobSnapShotDemo
{
    class Program
    {
        private const string storageConnectionString = "UseDevelopmentStorage=true";
        
        //private const string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=YourStorageAccount;AccountKey=YourPrimaryKey";

        static void Main(string[] args)
        {
            CreateSnapshotUsingClientLibrary();

            //CreateSnapshotUsingREST();            

            //ListSnapshotsForBlob();

            //ListSnapshotsUsingREST();

            //RestoreFromSnapshot();

            //RestoreFromSnapshotUsingREST();

            //DeleteSnapshotForBlob();

            //DeleteSnapshotForBlobUsingREST();

            Console.ReadLine();
        }

        private static void CreateSnapshotUsingClientLibrary()
        {
            //specifying container name and blob name - change them as per your blob and container name
            string containerName = "mycontainer";
            string blobName = "AzureBootCamp.zip";

            // Retrieve storage account from connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Get a reference to the container  and blob
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            
            //create snapshot
            CloudBlockBlob snapshot = blob.CreateSnapshot();

            //list down the Uri of snapshot created
            Console.WriteLine("SnapshotQualifiedUri: " + snapshot.SnapshotQualifiedUri);
            Console.WriteLine("Snap shot time:" + snapshot.SnapshotTime);
        }

        private static void CreateSnapshotUsingREST()
        {
            try
            {
                //specifying container name and blob name - change them as per your blob and container name
                string containerName = "mycontainer";
                string blobName = "AzureBootCamp.zip";

                string contentType = string.Empty;
                string snapshotTime = string.Empty;
                DateTime now = DateTime.UtcNow;

                //to perform any operation first lets generate the SAS url on the container, validity 1 minute
                SASManager sasMgr = new SASManager(storageConnectionString);
                string sasUrl = sasMgr.GetAdHocWriteOperationSAS(containerName, blobName);

                //perform operation to create snapshot
                HttpWebRequest requestCreateSnapshot = (HttpWebRequest)WebRequest.Create(sasUrl + "&comp=snapshot");
                requestCreateSnapshot.ContentLength = 0;                
                requestCreateSnapshot.Headers.Add("x-ms-version", "2014-02-14");
                requestCreateSnapshot.Headers.Add("x-ms-date", now.ToString("R", System.Globalization.CultureInfo.InvariantCulture));
                requestCreateSnapshot.Method = "PUT";

                using (HttpWebResponse respCreateSnapshot = (HttpWebResponse)requestCreateSnapshot.GetResponse())
                {
                    if (respCreateSnapshot.StatusCode == HttpStatusCode.Created)//create operation returns CREATED response
                    {
                        if (respCreateSnapshot.Headers != null)
                        {
                            snapshotTime = respCreateSnapshot.Headers.Get("x-ms-snapshot");
                        }
                    }
                }
                Console.WriteLine("snapshot time - " + snapshotTime);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private static void ListSnapshotsForBlob()
        {
            //specifying container name and blob name - change them as per your blob and container name
            string containerName = "mycontainer";
            string blobName = "AzureBootCamp.zip";

            // Retrieve storage account from connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Get a reference to the container  and blob
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            
            //retrive list of snapshots
            IList<IListBlobItem> snapshots = container.ListBlobs(null, true, BlobListingDetails.Snapshots).Where(x => ((CloudBlockBlob)x).IsSnapshot).ToList();

            //write total number of snapshots for blob
            Console.WriteLine("Total snapshots:" + snapshots.Count + Environment.NewLine);
            
            foreach (IListBlobItem snapshot in snapshots)
            {                
                Console.WriteLine("Snapshot Uri:" + ((CloudBlockBlob)snapshot).SnapshotQualifiedUri + Environment.NewLine + "Snapshot Timestamp:" + ((CloudBlockBlob)snapshot).SnapshotTime);
            }
        }

        private static void ListSnapshotsUsingREST()
        {
            try
            {
                //specifying container name and blob name - change them as per your blob and container name
                string containerName = "mycontainer";
                List<string> snapshots = new List<string>();

                //to perform any operation first lets generate the SAS url on the container, validity 1 minute
                SASManager sasMgr = new SASManager(storageConnectionString);
                string sasUrl = sasMgr.GetAdHocListSnapshotsSAS(containerName);

                //perform operation to create snapshot
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(sasUrl + "&restype=container&comp=list&include=snapshots");
                request.ContentLength = 0;
                request.Headers.Add("x-ms-version", "2014-02-14");
                request.Method = "GET";

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    if (response.StatusCode == HttpStatusCode.OK)//list operation returns OK response
                    {
                        using (StreamReader reader = new StreamReader(response.GetResponseStream()))
                        {
                            string result = reader.ReadToEnd();

                            //read snapshot from xml
                            XElement x = XElement.Parse(result);
                            foreach (XElement blob in x.Element("Blobs").Elements("Blob"))
                            {
                                if (blob.Element("Snapshot") != null)
                                {
                                    snapshots.Add(blob.Element("Snapshot").Value);
                                }
                            }
                        }
                    }
                }
                //print snapshots name
                foreach (string snapshot in snapshots)
                {
                    Console.WriteLine("Snapshot Name:" + snapshot);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private static void RestoreFromSnapshot()
        {
            //specifying container name and destination blob name - change them as per your destination blob and container name
            string containerName = "mycontainer";            
            string destinationBlobName = "AzureBootCampRestored.zip";            

            // Retrieve storage account from connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Get a reference to the container and blob
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);            
            CloudBlockBlob destinationBlob = container.GetBlockBlobReference(destinationBlobName);

            //retrive the snapshot url from the first snapshot - here you can retrive or use snapshot url of your choice. For demo purpose I am considering the first snapshot here.
            string snapshotUri = ((CloudBlockBlob)(container.ListBlobs(null, true, BlobListingDetails.Snapshots).Where(x => ((CloudBlockBlob)x).IsSnapshot).FirstOrDefault())).SnapshotQualifiedUri.AbsoluteUri;
            //or you can specify the snapshot uri of your choice as below
            //string snapshotUri = "http://127.0.0.1:10000/devstoreaccount1/mycontainer/AzureBootCamp.zip?snapshot=2014-09-19T05:29:50.4570000Z";            

            //perform copy/restore operation from snapshot uri
            string taskId = destinationBlob.StartCopyFromBlob(new Uri(snapshotUri));

            Console.WriteLine("Restore blob url task Id:" + taskId);

            while (destinationBlob.CopyState.Status == CopyStatus.Pending)
            {
                Task.Delay(TimeSpan.FromSeconds(20d)).Wait();
                destinationBlob = (CloudBlockBlob)container.GetBlobReferenceFromServer(destinationBlobName);
            }
            Console.WriteLine("Copy operation complete");
        }

        private static void RestoreFromSnapshotUsingREST()
        {
            try
            {
                //specifying container name and blob name - change them as per your blob and container name
                string containerName = "mycontainer";//my source and destination containers are same.
                string blobName = "AzureBootCamp.zip";                
                string destinationBlobName = "AzureBootCampRestored.zip";
                string copyStatus = string.Empty;                

                //to perform any operation first lets generate the SAS url on the source container-blob, validity 1 minute
                SASManager sasMgr = new SASManager(storageConnectionString);
                string sasUrl = sasMgr.GetAdHocWriteOperationSAS(containerName, blobName);

                //create sas on destination container-blob
                string destinationSASUrl = sasMgr.GetAdHocWriteOperationSAS(containerName, destinationBlobName);

                //create request for destination blob
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(destinationSASUrl);
                request.ContentLength = 0;                
                request.Headers.Add("x-ms-version", "2014-02-14");
                //add source information - this will be snapshot in our case. The uri query parameter of snapshot will be different for your snapshot
                request.Headers.Add("x-ms-copy-source", sasUrl + "&snapshot=2014-09-19T05:29:50.4570000Z");
                request.Method = "PUT";

                using (HttpWebResponse respCreateSnapshot = (HttpWebResponse)request.GetResponse())
                {
                    if (respCreateSnapshot.StatusCode == HttpStatusCode.Accepted)//create operation returns ACCEPTED response
                    {
                        if (respCreateSnapshot.Headers != null)
                        {
                            //retrive copy state information from header
                            copyStatus = respCreateSnapshot.Headers.Get("x-ms-copy-status");
                            while (copyStatus != "success")
                            {
                                copyStatus = respCreateSnapshot.Headers.Get("x-ms-copy-status");
                                Thread.Sleep(500);
                            }
                        }
                    }
                }
                Console.WriteLine("restore operation status- " + copyStatus);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private static void DeleteSnapshotForBlob()
        {
            try
            {
                //list the total number of snapshot present currently
                ListSnapshotsForBlob();

                //specifying container name and blob name - change them as per your blob and container name
                string containerName = "mycontainer";
                string blobName = "AzureBootCamp.zip";                

                // Retrieve storage account from connection string
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

                // Create the blob client
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

                // Get a reference to the container and blob snapshot
                CloudBlobContainer container = blobClient.GetContainerReference(containerName);
                DateTimeOffset? snapshotTime = ((CloudBlockBlob)(container.ListBlobs(null, true, BlobListingDetails.Snapshots).Where(x => ((CloudBlockBlob)x).IsSnapshot).FirstOrDefault())).SnapshotTime;

                CloudBlockBlob snapshot = container.GetBlockBlobReference(blobName, snapshotTime);

                #region Delete individual snapshot

                //delete individual snapshot
                snapshot.Delete();
                Console.WriteLine("Individual Snapshot delete operation complete");

                //list total number snapshot remaining after deleting one
                ListSnapshotsForBlob();

                #endregion

                #region Delete all snapshots but retain original blob

                CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
                blob.Delete(DeleteSnapshotsOption.DeleteSnapshotsOnly);
                ListSnapshotsForBlob();

                #endregion
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static void DeleteSnapshotForBlobUsingREST()
        {
            try
            {
                //specifying container name and blob name - change them as per your blob and container name
                string containerName = "mycontainer";//my source and destination containers are same.
                string blobName = "AzureBootCamp.zip";                                

                //to perform any operation first lets generate the SAS url on the source container-blob, validity 1 minute
                SASManager sasMgr = new SASManager(storageConnectionString);
                string sasUrl = sasMgr.GetAdHocWriteOperationSAS(containerName, blobName);                

                //create request for destination blob
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(sasUrl);
                request.ContentLength = 0;
                request.Headers.Add("x-ms-version", "2014-02-14");
                //specify to delete all snapshots but retain the blob
                request.Headers.Add("x-ms-delete-snapshots", "only");
                request.Method = "DELETE";

                using (HttpWebResponse respCreateSnapshot = (HttpWebResponse)request.GetResponse())
                {
                    if (respCreateSnapshot.StatusCode == HttpStatusCode.Accepted)//delete operation returns ACCEPTED response
                    {                        
                    }
                }
                Console.WriteLine("delete snapshots operation successfull");
                ListSnapshotsForBlob();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

    }
}
