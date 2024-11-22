# Databricks notebook source
# Mounting attempt using Databricks managed identity 'DatabricksManagedIdentity-proje2e' client id and client secret in registered app 
#  Define the configurations
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": {},
  "fs.azure.account.oauth2.client.secret": {},
  "fs.azure.account.oauth2.client.endpoint": 'https://login.microsoftonline.com/',{},'/oauth2/token'
}

# Mount the storage
dbutils.fs.mount(
  source = "abfss://tesingdatabricks@proje2e.dfs.core.windows.net/",
  mount_point = "/mnt/tesingdatabricks",
  extra_configs = configs
)

# COMMAND ----------

dbutils.fs.ls('/mnt/tesingdatabricks')

# COMMAND ----------

storageAccountName = "proje2e"
storageAccountAccessKey = {}
sasToken = {}
adlsContainerName = "tesingdatabricks"
mountPoint = "/mnt/tesingdatabricks1/"
if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.unmount(mountPoint)
try:
  dbutils.fs.mount(
    source = "abfss://{}@{}.dfs.core.windows.net".format(adlsContainerName, storageAccountName),
    mount_point = mountPoint,
    #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.dfs.core.windows.net': storageAccountAccessKey}
    extra_configs = {'fs.azure.sas.' + adlsContainerName + '.' + storageAccountName + '.dfs.core.windows.net': sasToken}
  )
  print("mount succeeded!")
except Exception as e:
  print("mount exception", e)
