# Databricks notebook source
def get_mount_point(storageAccountName, list_containers):

  # Check if the storage account name and list of containers are provided
    if not storageAccountName or not list_containers:
        raise ValueError("Both storage account name and list of containers must be provided.")
    else :
        # Mounting using Databricks managed identity 'DatabricksManagedIdentity-proje2e' client id and client secret in registered app 
        #  Define the configurations
        configs = {
          "fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": {},
          "fs.azure.account.oauth2.client.secret": {},
          "fs.azure.account.oauth2.client.endpoint": 'https://login.microsoftonline.com/',{},'/oauth2/token'
        }

        for adlsContainerName in list_containers:
          mountPoint =  "/mnt/"+ adlsContainerName
          # Mount the storage
          if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()): 
            dbutils.fs.unmount(mountPoint)  #if mount point already exists, then first unmount it
          try:
            dbutils.fs.mount(
            source = "abfss://{}@{}.dfs.core.windows.net/".format(adlsContainerName, storageAccountName),
            mount_point = mountPoint,
            extra_configs = configs
          )
            print("mount succeeded for"+mountPoint)
          except Exception as e:
            print("mount exception", e)
