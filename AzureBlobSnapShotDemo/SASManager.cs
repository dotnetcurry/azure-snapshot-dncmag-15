using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureBlobSnapShotDemo
{
    public class SASManager
    {
        CloudStorageAccount storageAccount = null;
        public SASManager(string storageConnectionString)
        {
            // Retrieve storage account from connection string
            storageAccount = CloudStorageAccount.Parse(storageConnectionString);
        }

        /// <summary>
        /// method to create write operation SAS on azure container
        /// </summary>
        /// <param name="containerName"></param>
        /// <returns></returns>
        public string GetAdHocWriteOperationSAS(string containerName, string blobName)
        {
            string existingPolicyKey = string.Empty;
            string sasBlobToken = string.Empty;

            // Create blob container permissions, consisting of a shared access policy 
            BlobContainerPermissions blobPermissions = new BlobContainerPermissions();

            // Create the queue client
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Get a reference to the container for which shared access signature will be created.
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);

            //creat write operation permissions on container which are valid for 2 minutes
            SharedAccessBlobPolicy sasConstraint = new SharedAccessBlobPolicy();
            sasConstraint.SharedAccessStartTime = DateTime.UtcNow.AddMinutes(-1);
            sasConstraint.SharedAccessExpiryTime = DateTime.UtcNow.AddMinutes(2);
            //sasConstraint.Permissions = SharedAccessBlobPermissions.Write | SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Delete;
            sasConstraint.Permissions = SharedAccessBlobPermissions.Write | SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Delete;

            string adHocSAS = blob.GetSharedAccessSignature(sasConstraint);

            string adHocSASUrl = blob.Uri + adHocSAS;
            return adHocSASUrl;
        }

        public string GetAdHocListSnapshotsSAS(string containerName)
        {
            string existingPolicyKey = string.Empty;
            string sasBlobToken = string.Empty;

            // Create blob container permissions, consisting of a shared access policy 
            BlobContainerPermissions blobPermissions = new BlobContainerPermissions();

            // Create the queue client
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Get a reference to the container for which shared access signature will be created.
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            //CloudBlockBlob blob = container.GetBlockBlobReference(blobName);

            //creat write operation permissions on container which are valid for 2 minutes
            SharedAccessBlobPolicy sasConstraint = new SharedAccessBlobPolicy();
            sasConstraint.SharedAccessStartTime = DateTime.UtcNow.AddMinutes(-1);
            sasConstraint.SharedAccessExpiryTime = DateTime.UtcNow.AddMinutes(2);
            //sasConstraint.Permissions = SharedAccessBlobPermissions.Write | SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Delete;
            sasConstraint.Permissions = SharedAccessBlobPermissions.List | SharedAccessBlobPermissions.Read;

            string adHocSAS = container.GetSharedAccessSignature(sasConstraint);

            string adHocSASUrl = container.Uri + adHocSAS;
            return adHocSASUrl;
        }        
    }
}
