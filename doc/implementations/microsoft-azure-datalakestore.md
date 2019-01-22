# Microsoft Azure Data Lake Store

Microsoft Azure implementations reside in a separate package hosted on [NuGet](https://www.nuget.org/packages/Storage.Net.Microsoft.Azure.DataLake.Store/). Follow the link for installation instructions.

This package tries to abstract access to [Azure Data Lake Store](https://azure.microsoft.com/en-gb/services/data-lake-store/) and making them available as `IBlobStorage`. It's internals are heavily optimised for performance and includes a lot of workarounds in order to make the official ADLS SDK actually be useful to a user without spending weeks on StackOverflow. 

## Using

This library supports service-to-service authentication with **client secret**. This authentication requires the following parts:

- **Account name** is the name of the ADLS account as displayed in the portal: ![Adl 04](adl-04.png)
- **TenantId** is active directory __tenant id__
- **PrincipalId** is active directory principal's __client id__ (application id)
- **PrincipalSecret** is active directory principal's __secret__

Please see appendix at the end of this page which helps to set this up for the first time.

## Connecting

### With code

```csharp
IBlobStorage storage = StorageFactory.Blobs.AzureDataLakeStoreByClientSecret(
	"account_name", "tenantId", "principalId", "principalSecret");
```

### With connection string

```csharp
// do not forget to initialise azure module before your application uses connection strings:
StorageFactory.Modules.UseAzureDataLakeStorage();

// create the storage
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("azure.datalakestore://accountName=...;tenantId=...;principalId=...;principalSecret=...");
```

### Important Implementation Notes

- Uploading a file always overwrites existing file if it exists, otherwise a new file is created. This still takes one network call.
- Appending to a file checks if a file exists first, so this operation results in two network calls.
- List operation supports folder hierary, folders, recursion, and limiting by number of items, i.e. no limitations whatsoever.
- Application that creates an instance of IBlobStorage is highly recomended to set `ServicePointManager.DefaultConnectionLimit` to the number of threads application wants the sdk to use before creating any instance of IBlobStorage. By default ServicePointManager.DefaultConnectionLimit is set to 2 by .NET runtime which might be a small number.


## Appendix. Creating a Service Principal

### For authentication using Client Secret

#### Create Active Directory Application

Go to **Active Directory / App registrations**:

![Adl 00](adl-00.png)

Press **Add button**. Type any name for the application, but leave **Application Type** as **Web app / API** and set **sign-on url** to **`http://localhost`**:

![Adl 01](adl-01.png)

Now open the application and create a new key. We need to write down this **key** and **Application ID**:

![Adl 02](adl-02.png)

You can get the **tenant id** (sometimes called a **diretory id** or **domain**) from the directory properties:

![Adl 03](adl-03.png)
