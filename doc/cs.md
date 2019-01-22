# Connection Strings

Connection string allows you to create an implementation of blob storage, table storage or messaging using just a string. Connection string consists of a **prefix** that helps this library to decide which implementation to create and is essentially a mapping of implementation name to the implementation, a prefix separator `://` and a set of *key-value pairs* that are implementation specific.

Key-value pairs are separated by `;` sign and key and values are separated by '=' sign. A full connection string will look like:

`prefix`://`key1`=`value1`;`key2`=`value2` and so on.

Note that all of the keys are considered to be **case-insensitive**.

## Blob Storage

This page lists known connection strings for different blob storage implementations

|Technology/Prefix|Argument|Required|Notes|
|-|-|-|-|
|**Azure Blob Storage**/azure.blob|account|yes||
||key|yes||
||container|no|when set, maps storage implementation to a specific container|
|**Azure Data Lake Storage**/azure.datalakestore|accountName|yes||
||tenantId|yes||
||principalId|yes||
||principalSecret|yes||
||listBatchSize|no|when set, overrides default batch size for blob listing operations requests|
