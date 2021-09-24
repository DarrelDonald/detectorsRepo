private static string GetQuery(OperationContext<App> cxt)
{
string siteName = cxt.Resource.Name;
    return
    $@"AntaresIISLogWorkerTable 
| where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource, "TIMESTAMP")}
            | where S_sitename =~ '{siteName}' or S_sitename =~ '{siteName}__' 			
            | summarize Requests = count() by RoleInstance, Tenant
            |join kind = inner        
            (
                RoleInstanceHeartbeat 
| where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource, "TIMESTAMP")}
                    | summarize  MachineName = any(MachineName), InstanceId = any(InstanceId) by Tenant, RoleInstance
            ) 
            on RoleInstance, Tenant
            | project RoleInstance, MachineName, Requests";
}

private static string GetQueryGraph(OperationContext<App> cxt)
{
string siteName = cxt.Resource.Name;
    return
    $@"AntaresIISLogWorkerTable 
            | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource, "TIMESTAMP")}
            | where S_sitename =~ '{siteName}' or S_sitename =~ '{siteName}__' 			
            | summarize Requests = count() by RoleInstance, Tenant,  bin(TIMESTAMP, 10m)
            |join kind = inner        
            (
                RoleInstanceHeartbeat 
                    | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource, "TIMESTAMP")}
                    | summarize  MachineName = any(MachineName), InstanceId = any(InstanceId) by Tenant, RoleInstance
            ) 
            on RoleInstance, Tenant
            | project RoleInstance, TIMESTAMP, Requests";
}

[AppFilter(AppType = AppType.WebApp, PlatformType = PlatformType.Windows, StackType = StackType.All)]
[Definition(Id = "ErinDemo1", Name = "Requests Per Instance - Demo", Author = "khzayed, yunjchoi", Description = "Demo Detector")]
public async static Task<Response> Run(DataProviders dp, OperationContext<App> cxt, Response res)
{
    var instanceTable =  await dp.Kusto.ExecuteQuery(GetQuery(cxt), cxt.Resource.Stamp.Name);
    var instancesNumber = Int32.Parse(instanceTable.Rows.Count.ToString());
    res.Dataset.Add(new DiagnosticData()
    {
    Table = instanceTable,
    RenderingProperties = new TableRendering(),
    });

   
    res.Dataset.Add(new DiagnosticData()
    {
    Table = await dp.Kusto.ExecuteQuery(GetQueryGraph(cxt), cxt.Resource.Stamp.Name),
    RenderingProperties = new TimeSeriesRendering(){
        Title = "Requests per Instance",
        GraphType = TimeSeriesType.LineGraph
    },
    });


    res.AddInsight(InsightStatus.Info, "Your app is running on " + instancesNumber + " instances");
     if (cxt.IsInternalCall)
     {
        res.AddInsight(InsightStatus.Info, "Internal view");
     }
     else
     {
                 res.AddInsight(InsightStatus.Info, "External view");
     }

    return res;
}