# Databricks



## Notebooks

### Exporting Notebooks

Notebooks can be exported by calling to `OpenReadAsync` or it's related methods like `ReadTextAsync`. By default notebooks are exported as source code, however you can override that by appending a format specifier at the end of the path.

For instance, in order to export in Jupyter format, you would write the following:

```csharp
string path = "/workspace/Shared/mynotebook"
string jupyterData = await _storage.ReadTextAsync(path + "#jupyter");
```

The supported formats are:

- source
- html
- jupyter
- dbc