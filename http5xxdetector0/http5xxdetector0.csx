#load "geoutilities"

using System;
using System.Data;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Diagnostics.DataProviders;
using Diagnostics.ModelsAndUtils;
using Diagnostics.ModelsAndUtils.Attributes;
using Diagnostics.ModelsAndUtils.Models;
using Diagnostics.ModelsAndUtils.Models.ResponseExtensions;
using Diagnostics.ModelsAndUtils.ScriptUtilities;
using Newtonsoft.Json;
using System.Linq;
using System.Text;

private static string GetRunningInstanceDetails(OperationContext<App> cxt)
{
    return
    $@"StatsDWASWorkerProcessTenMinuteTable 
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource, "TIMESTAMP")} 
        | where ApplicationPool =~ ""{cxt.Resource.Name}"" or ApplicationPool startswith ""{cxt.Resource.Name}__""
        | distinct Tenant, RoleInstance
        | join kind=inner (
            RoleInstanceHeartbeat 
            | where  {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)} 
            | distinct Tenant, RoleInstance , MachineName, Details, InstanceId
            | project-rename IP = Details 
        ) on RoleInstance , Tenant
        | project Tenant , RoleInstance , MachineName, IP, InstanceId
        | order by RoleInstance asc"; 
}

private static string GetHttp5xxErrorsPerInstance(OperationContext<App> cxt)
{
    //Workaround a known issue related to HostNamesFilterQuery
    var hostnamesQuery = Utilities.HostNamesFilterQuery(cxt.Resource.Hostnames);
    hostnamesQuery = hostnamesQuery.Replace("\"orCs_host", "\" or Cs_host");

    return
    $@"AntaresIISLogFrontEndTable 
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
        | where {hostnamesQuery}
        | where User_agent != ""AlwaysOn"" and User_agent!=""Azure+Traffic+Manager+Endpoint+Monitor""
        | where Sc_status >= 500
        | project TIMESTAMP, IP = ServerRouted, Sc_status, SiteRuntimeName = S_sitename 
        | summarize Count = count() by IP
        | join kind=inner (
            RoleInstanceHeartbeat 
            | where  {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)} 
            | extend IP = Details 
            | distinct IP, RoleInstance, Tenant
        ) on IP
        | project Tenant, RoleInstance, Count
        | order by RoleInstance asc"; 
}

private static string GetHttp5xxDetailsPerWorker(OperationContext<App> cxt)
{
    //Workaround a known issue related to HostNamesFilterQuery
    var hostnamesQuery = Utilities.HostNamesFilterQuery(cxt.Resource.Hostnames);
    hostnamesQuery = hostnamesQuery.Replace("\"orCs_host", "\" or Cs_host");

    return
    $@"let sites = AntaresIISLogFrontEndTable
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
        | where {hostnamesQuery}
        | where S_sitename != ""-""
        | order by PreciseTimeStamp asc
        | summarize makeset(S_sitename);
    (AntaresIISLogWorkerTable 
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
        | where S_sitename in~ (toscalar(sites))
        | where User_agent != ""AlwaysOn"" and User_agent!=""Azure+Traffic+Manager+Endpoint+Monitor""
        | where Sc_status >= 500
        | project TIMESTAMP, Tenant, RoleInstance, Sc_status, SiteRuntimeName = S_sitename, StatusCode = strcat(Sc_status, ""."", iif(isempty(Sc_substatus), ""0"", Sc_substatus)), Sc_win32_status,S_reason)
    | union 
        (AntaresIISLogWorkerTable 
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
        | where S_sitename in~ (toscalar(sites))
        | where User_agent != ""AlwaysOn"" and User_agent!=""Azure+Traffic+Manager+Endpoint+Monitor""
        | where EventId == 44101
        | where Sc_status < 500
        | project TIMESTAMP, Tenant, RoleInstance, Sc_status, SiteRuntimeName = S_sitename, StatusCode = strcat(Sc_status, ""."", iif(isempty(Sc_substatus), ""0"", Sc_substatus)), Sc_win32_status,S_reason
        )
        | summarize Count = count() by Tenant, RoleInstance, SiteRuntimeName ,StatusCode, Sc_win32_status,S_reason
        | order by RoleInstance asc, Count desc"; 
}

private static string GetHttp5xxDetailsFrontend(OperationContext<App> cxt)
{
    //Workaround a known issue related to HostNamesFilterQuery
    var hostnamesQuery = Utilities.HostNamesFilterQuery(cxt.Resource.Hostnames);
    hostnamesQuery = hostnamesQuery.Replace("\"orCs_host", "\" or Cs_host");

    return
    $@"AntaresIISLogFrontEndTable 
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
        | where {hostnamesQuery}
        | where User_agent != ""AlwaysOn"" and User_agent!=""Azure+Traffic+Manager+Endpoint+Monitor""
        | where Sc_status >= 500
        | project TIMESTAMP, IP = ServerRouted, Sc_status, SiteRuntimeName = S_sitename, StatusCode = strcat(Sc_status, ""."", Sc_substatus), Sc_win32_status,S_reason
        | summarize Count = count() by IP, SiteRuntimeName, StatusCode, Sc_win32_status,S_reason
        | join kind=inner (
            RoleInstanceHeartbeat 
            | where  {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)} 
            | extend IP = Details 
            | distinct IP, RoleInstance, Tenant
        ) on IP
        | project Tenant, RoleInstance, Count, SiteRuntimeName, StatusCode, Sc_win32_status,S_reason
        | order by RoleInstance asc, Count desc"; 
}


private static string GetHttp5xxCountPerInstancePer5Minutes(OperationContext<App> cxt)
{
    //Workaround a known issue related to HostNamesFilterQuery
    var hostnamesQuery = Utilities.HostNamesFilterQuery(cxt.Resource.Hostnames);
    hostnamesQuery = hostnamesQuery.Replace("\"orCs_host", "\" or Cs_host");

    return
    $@"AntaresIISLogFrontEndTable 
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
        | where {hostnamesQuery}
        | where User_agent != ""AlwaysOn"" and User_agent!=""Azure+Traffic+Manager+Endpoint+Monitor""
        | where Sc_status >= 500
        | project PreciseTimeStamp, IP = ServerRouted, Sc_status, SiteRuntimeName = S_sitename 
        | join kind=inner (
            RoleInstanceHeartbeat 
            | where  {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)} 
            | extend IP = Details 
            | distinct IP, RoleInstance, Tenant, MachineName 
        ) on IP
        | project PreciseTimeStamp, Tenant, RoleInstance, SiteRuntimeName
        | summarize Count = count() by Tenant, RoleInstance, bin(PreciseTimeStamp, 5m)
        | order by RoleInstance asc, Count desc
        | extend MergedRoleInstance = strcat(substring(Tenant, 0, 6), "":"", iif(RoleInstance startswith ""SmallDedicatedWebWorkerRole_IN_"", replace(""SmallDedicatedWebWorkerRole_IN_"", ""SDW_"", RoleInstance), iif(RoleInstance startswith ""MediumDedicatedWebWorkerRole_IN_"", replace(""MediumDedicatedWebWorkerRole_IN_"", ""MDW_"", RoleInstance),replace(""LargeDedicatedWebWorkerRole_IN_"", ""LDW_"", RoleInstance))))
        | project-away Tenant , RoleInstance 
        | project PreciseTimeStamp, RoleInstance = MergedRoleInstance, Count"; 
}

[AppFilter(AppType = AppType.ApiApp|AppType.MobileApp |AppType.WebApp, PlatformType = PlatformType.Windows, StackType = StackType.All)]
[Definition(Id = "Http5xxDetector0", Name = "Troubleshoot Http 5xx errors", Category=Categories.AvailabilityAndPerformance, Author = "xinjin", Description = "")]
public async static Task<Response> Run(DataProviders dp, OperationContext<App> cxt, Response res)
{

    var hostingEnv = cxt.Resource.Stamp;
    string stampName = !string.IsNullOrWhiteSpace(hostingEnv.InternalName) ? hostingEnv.InternalName : hostingEnv.Name;

    string[] ColourValues = new string[] {
        "#1F77B4", "#AEC7E8", "#FF7F0E", "#FFBB78", "#2CA02C", "#98DF8A",
        "#D62728", "#FF9896", "#9467BD", "#C5B0D5", "#8C564B", "#C49C94", 
        "#E377C2", "#F7B6D2", "#7F7F7F", "#C7C7C7", "#BCBD22", "#DBDB8D",
        "#17BECF", "#9EDAE5" 
    };     
 
    Dictionary<string, string> RegionClusterNameCollection = new Dictionary<string, string>();
    GeoUtilities.InitializeRegionClusterNameCollection(RegionClusterNameCollection);
    string regionKey = cxt.Resource.Stamp.Location.ToUpper();
    string cluster = RegionClusterNameCollection[regionKey];

    //Tasks
    var http5xxErrorsPerInstanceTask = dp.Kusto.ExecuteQuery(GetHttp5xxErrorsPerInstance(cxt), stampName, null, "GetHttp5xxErrorsPerInstance");
    var runningInstanceDetailsTask = dp.Kusto.ExecuteQuery(GetRunningInstanceDetails(cxt), stampName, null, "GetRunningInstanceDetails");
    var http5xxCountPerInstancePer5MinutesTask = dp.Kusto.ExecuteQuery(GetHttp5xxCountPerInstancePer5Minutes(cxt), stampName, null, "GetHttp5xxCountPerInstancePer5Minutes");
    var http5xxDetailsFrontendTask = dp.Kusto.ExecuteQuery(GetHttp5xxDetailsFrontend(cxt), stampName, null, "GetHttp5xxDetailsFrontend");    
    var http5xxDetailsPerWorker = dp.Kusto.ExecuteQuery(GetHttp5xxDetailsPerWorker(cxt), stampName, null, "GetHttp5xxDetailsPerWorker");

    var http5xxErrorsPerInstanceTable = await http5xxErrorsPerInstanceTask;

    //Insight
    var inStatus = InsightStatus.Success;

    var insightOverview = new Dictionary<string,string>();

    string http5xxOverview = $"A majority of issues in this area are caused by the application code, or a framework/library component that is not part of the App Service platform. As a general rule, our goal is to identify whether the cause of the error is an App Service (platform) issue or an application issue." + Environment.NewLine + Environment.NewLine;
    string requestLifetime = $"It is helpful to think of the lifetime of the request as it traverses the App Service when deciding an issue is a platform issue and when it becomes an application issue. The general flow is: *FrontEnd* -> *Worker* -> *Web server process (IIS/Apache)* -> *(optional) external framework process (ASP.Net Core, Java, PHP, etc.)* -> *framework/technology stack (ASP.Net, PHP, Node.js etc.)* -> *application code*." + Environment.NewLine + Environment.NewLine;
    string applensUrl = $"Generally we can start to troubleshoot from [Web App Down]({GetAppLensV3DetectorUrl("webappdown", cxt)}). Then you can drill down in this detector for HTTP 5xx errors";

    string http5xxOverviewInsight = $"<markdown>{http5xxOverview}{requestLifetime}{applensUrl}</markdown>";
    insightOverview.Add("Http 5xx Overview", http5xxOverviewInsight);

    var insightHttp5xxTitle = "No Http 5xx Detected";
    if(http5xxErrorsPerInstanceTable.Rows.Count > 0)
    {
        inStatus = InsightStatus.Critical;
        insightHttp5xxTitle = "Http 5xx Errors Detected";
    }

    insightOverview.Add("Troubleshooting", $@"<markdown>[Troubleshoot HTTP errors of ""502 bad gateway"" and ""503 service unavailable"" in Azure App Service](https://docs.microsoft.com/en-us/azure/app-service/troubleshoot-http-502-http-503)</markdown>");    

    var runningInstanceDetailsTable = await runningInstanceDetailsTask;

    var vmsMarkdown = DataTableToMarkdown(runningInstanceDetailsTable, cluster, cxt);
    string vmsDescription = $"The below table shows all VM instances which were running the web app in the time frame. **Ctrl+Click an instance below** directs you to *Jarvis Worker Dashboard* where you will get a quick overview of basic worker signals." + Environment.NewLine + Environment.NewLine;
    string vmsInsight = $"<markdown>{vmsDescription}{vmsMarkdown}</markdown>";
    insightOverview.Add("Additional Information", vmsInsight);
    

    Insight insightHttp5xx = new Insight(inStatus, insightHttp5xxTitle, insightOverview);
    insightHttp5xx.IsExpanded = true;
    res.AddInsights(new List<Insight>() { insightHttp5xx });

    if(http5xxErrorsPerInstanceTable.Rows.Count <= 0)
    {
        return res;
    }


    var summary = new List<DataSummary>();

    for(int i = 0; i<http5xxErrorsPerInstanceTable.Rows.Count; i++)
    {
        string color = ColourValues[i%ColourValues.Length];
        DataSummary s = new DataSummary(NormalizeWorkerInstanceName(http5xxErrorsPerInstanceTable.Rows[i]["RoleInstance"].ToString(), http5xxErrorsPerInstanceTable.Rows[i]["Tenant"].ToString()), http5xxErrorsPerInstanceTable.Rows[i]["Count"].ToString(), color);
        summary.Add(s);            
    }

    res.AddDataSummary(summary);

    //Chart of Http 5xx per instance
    res.Dataset.Add(new DiagnosticData()
    {
        Table = await http5xxCountPerInstancePer5MinutesTask,
        RenderingProperties = new TimeSeriesRendering()
        {
            Title = "Http 5xx per instance",
            GraphType = TimeSeriesType.LineGraph
        }

    });        


    var http5xxDeetailsFrontendTable = await http5xxDetailsFrontendTask;
    var dictHttp5xxFrontendResult = BuildHttp5xxDictionary(http5xxDeetailsFrontendTable);
    Dropdown dropdownViewModelFrontend = new Dropdown("Select the instance", BuildHttp5xxDropDown(dictHttp5xxFrontendResult));
    res.AddDropdownView(dropdownViewModelFrontend, "Http 5xx details that were logged on the Frontend");



    var http5xxDetailsPerWorkerTable = await http5xxDetailsPerWorker;
    var dictHttp5xxWorkerResult = BuildHttp5xxDictionary(http5xxDetailsPerWorkerTable);
    Dropdown dropdownViewModelWorker = new Dropdown("Select the instance", BuildHttp5xxDropDown(dictHttp5xxWorkerResult));
    res.AddDropdownView(dropdownViewModelWorker, "Http 5xx details that were logged on the Workers");

    res.AddDetectorCollection(new List<string>() { "httperrlog"});

    return res;

}

private static string NormalizeWorkerInstanceName(string worker, string tenant)
{
    //e.g. tenant: e3f237bc4abe4a328d425e2a872cf3e2, LargeDedicatedWebWorkerRole_IN_23
    // return e3f237:LDW_23
    var outWorker = worker.Replace("LargeDedicatedWebWorkerRole_IN_", "LDW_")
                        .Replace("MediumDedicatedWebWorkerRole_IN_", "MDW_")
                        .Replace("SmallDedicatedWebWorkerRole_IN_", "SDW_");
    var outTenant = tenant.Substring(0, 6);
    
    return $@"{outTenant}:{outWorker}";

}

private static string GetAppLensV2AppAnalysisUrl(OperationContext<App> cxt)
{

    var hostingEnv = cxt.Resource.Stamp;
    string stampName = !string.IsNullOrWhiteSpace(hostingEnv.InternalName) ? hostingEnv.InternalName : hostingEnv.Name;

    string siteName;
    if(String.Equals(cxt.Resource.Slot, "Production", StringComparison.OrdinalIgnoreCase))
        siteName = cxt.Resource.Name;
    else
        siteName = $@"{cxt.Resource.Name}({cxt.Resource.Slot})";

    var startTime = DateTime.ParseExact(cxt.StartTime, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);
    var endTime = DateTime.ParseExact(cxt.EndTime, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);
    var result = $@"https://applensv2.azurewebsites.net/stamps/{stampName}/sites/{siteName}/appanalysis?startTime={startTime.ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture)}&endTime={endTime.ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture)}";
    return result;
}
private static string DataTableToMarkdown(DataTable dt, string cluster, OperationContext<App> cxt)
{
    var markDownBuilder = new StringBuilder();
    List<string> columns = new List<string>();
    markDownBuilder.AppendLine(string.Join(" | ", dt.Columns.Cast<DataColumn>().Select(c=>c.ColumnName)));
    string columnHeader = new StringBuilder().Insert(0, " --- |", dt.Columns.Count).ToString();
    columnHeader = columnHeader.Substring(1, columnHeader.Length -1);
    markDownBuilder.AppendLine(columnHeader);
    foreach(DataRow dr in dt.Rows)
    {
        dr["RoleInstance"] = $@"[{dr["RoleInstance"].ToString()}]({GetWorkerDashboardUrl(dr["Tenant"].ToString(), dr["RoleInstance"].ToString(), cluster, cxt)})";
        markDownBuilder.AppendLine(string.Join(" | ", dr.ItemArray));
    }
    return markDownBuilder.ToString();
} 

private static Dictionary<string, DataTable> BuildHttp5xxDictionary(DataTable source)
{
    var dictResult = new Dictionary<string, DataTable>();
    foreach(DataRow row in source.Rows)
    {
        string key = row["Tenant"].ToString() + ":" + row["RoleInstance"].ToString();
        DataTable keyValue;
        DataTable tempTable;
        bool keyExisted = true;
        if(dictResult.TryGetValue(key, out keyValue))
        {
            tempTable = (DataTable)keyValue;
        }
        else
        {
            keyExisted = false;
            tempTable = new DataTable();
            tempTable.Columns.Add("SiteRuntimeName");
            tempTable.Columns.Add("StatusCode");
            tempTable.Columns.Add("Sc_win32_status");            
            tempTable.Columns.Add("S_reason");            
            tempTable.Columns.Add("Count");            
        }

        var currRow = tempTable.NewRow();
        currRow["SiteRuntimeName"] = row["SiteRuntimeName"];
        currRow["StatusCode"] = row["StatusCode"];
        currRow["Sc_win32_status"] = row["Sc_win32_status"];
        currRow["S_reason"] = row ["S_reason"];
        currRow["Count"] = row["Count"];
        tempTable.Rows.Add(currRow);   

        if(!keyExisted)
            dictResult.Add(key, tempTable);
    }

    return dictResult;
}

private static List<Tuple<string, bool, Response>> BuildHttp5xxDropDown(Dictionary<string, DataTable> dictResult)
{
    List<Tuple<string, bool, Response>> data = new List<Tuple<string, bool, Response>>();
    bool selected = true;
    foreach(KeyValuePair<string, DataTable> kvp in dictResult)
    {
        var dataEntry = new Response();
        dataEntry.Dataset.Add(new DiagnosticData()
        {
            Table = kvp.Value,
            RenderingProperties = new Rendering(RenderingType.Table)
            {
                Title = kvp.Key
            }
        });

        var dropdownKeyParts = kvp.Key.Split(new char[] {':'});
        //data.Add(new Tuple<string, bool, Response>(dropdownKeyParts[1], selected, dataEntry));
        data.Add(new Tuple<string, bool, Response>($@"{dropdownKeyParts[0].Substring(0,6)}:{dropdownKeyParts[1]}", selected, dataEntry));

        if(selected)
            selected = false;
    }

    return data;
}

private static string GetWorkerDashboardUrl(string tenant, string roleInstance, string cluster ,OperationContext<App> cxt)
{
    double startMilliseconds = GetMillisecondsFromEpoch(cxt.StartTime);
    double endMilliseconds = GetMillisecondsFromEpoch(cxt.EndTime);

    var result = "https://jarvis-west.dc.ad.msft.net/dashboard/share/688762A6?overrides=[{%22query%22:%22//*[id=%27tenant%27]%22,%22key%22:%22value%22,%22replacement%22:%22"+tenant+"%22},{%22query%22:%22//*[id=%27roleInstance%27]%22,%22key%22:%22value%22,%22replacement%22:%22"+roleInstance+"%22},{%22query%22:%22//*[id=%27cluster%27]%22,%22key%22:%22value%22,%22replacement%22:%22"+cluster+"%22}]&globalStartTime="+ startMilliseconds.ToString() + "&globalEndTime=" + endMilliseconds.ToString() + "&pinGlobalTimeRange=true";

    return result;
}

private static double GetMillisecondsFromEpoch(string dt)
{
            DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            var startTime = DateTime.ParseExact(dt, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);
            return (startTime - epoch).TotalMilliseconds;

}


private static string GetAppLensV3DetectorUrl(string detectorName, OperationContext<App> cxt)
{
    string siteName;
    if(String.Equals(cxt.Resource.Slot, "Production", StringComparison.OrdinalIgnoreCase))
        siteName = cxt.Resource.Name;
    else
        siteName = $@"{cxt.Resource.Name}({cxt.Resource.Slot})";

    var startTime = DateTime.ParseExact(cxt.StartTime, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);
    var endTime = DateTime.ParseExact(cxt.EndTime, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);
    var result = $@"https://applens.azurewebsites.net/subscriptions/{cxt.Resource.SubscriptionId}/resourceGroups/{cxt.Resource.ResourceGroup}/sites/{siteName}/detectors/{detectorName}?startTime={startTime.ToString("yyyy-MM-ddTHH:mm", System.Globalization.CultureInfo.InvariantCulture)}&endTime={endTime.ToString("yyyy-MM-ddTHH:mm", System.Globalization.CultureInfo.InvariantCulture)}";
    return result;
}


