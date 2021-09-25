using System.Linq;
using System.Text;


//2

[AppFilter]
[Definition(Id = "webappdownwindows", Name = "webappdown-windows", Author = "puneetg", Description = "A gist that contains common functions for Availability and Performance Detectors")]

public static class Availability
{
    const string FakeStampForAnalyticsCluster = "waws-prod-blu-001";
    const string PlatformAvailabilityLabel = "Platform Availability";
    const string AppAvailabilityLabel = "App Availability";    
    public static int TimeGrain = 5;


    public static string GetSiteAndHostNamesQuery(OperationContext<App> cxt, bool isWorkerTableQuery = false)
    {
        string hostNamesQuery = Utilities.HostNamesFilterQuery(cxt.Resource.Hostnames);
        if (isWorkerTableQuery)
        {
            hostNamesQuery += $" or Cs_host =~ '{cxt.Resource.Name}'";
            hostNamesQuery += $" or Cs_host startswith '{cxt.Resource.Name}__'";
            return $@" where (S_sitename =~ '{cxt.Resource.Name}' or S_sitename startswith '{cxt.Resource.Name}__') or ({hostNamesQuery})
            | extend S_sitename = iif(S_sitename == '-', Cs_host, S_sitename)
            | where Cs_host !has '.scm.'";
        }
        else
        {
            return $@" where {hostNamesQuery}";
        }
        

    }

    public static string GetCanaryNamePrimaryQuery(OperationContext<App> cxt)
    {
        var YesterdayDate = GetDateTimeInUtcFormat(DateTime.UtcNow).AddDays(-2).ToString("yyyy-MM-dd");
        return
        $@"set query_results_cache_max_age = time(1d);cluster('wawseusfollower').database('wawsprod').WawsAn_dailyentity
                | where pdate == datetime({YesterdayDate})
                | where sitestamp == '{cxt.Resource.Stamp.InternalName}'
                | where sitename =~ '{cxt.Resource.Name}'
                | project sitesubscription, sitewhpid
                | join(
                cluster('wawseusfollower').database('wawsprod').WawsAn_dailyentity
                | where pdate == datetime({YesterdayDate})
                | where sitesubscription == '{cxt.Resource.SubscriptionId}'
                | where sitename startswith 'mawscanary' 
                | project CanaryName = sitename, sitewhpid, sitesubscription 
                  ) on sitesubscription, sitewhpid
                | project CanaryName";
    }

    public static string GetCanaryNameSecondaryQuery(OperationContext<App> cxt)
    {
        return
        $@"set query_results_cache_max_age = time(1d);
        StatsDWASWorkerProcessTenMinuteTable
            | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource, "TIMESTAMP")}
            | take 1
            | project TIMESTAMP, RoleInstance 
            | join(StatsDWASWorkerProcessTenMinuteTable
                | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource, "TIMESTAMP")}
                | where ApplicationPool startswith 'mawscanary'
                | project TIMESTAMP, RoleInstance, ApplicationPool  ) on TIMESTAMP, RoleInstance
            | summarize CanaryName = any(ApplicationPool)";
    }

    public static string[] GetCanaryHostNames(DataTable canaryHost)
    {
        var hostnameSuffix = new string[2]{"azurewebsites.net", "chinacloudsites.cn"};
        var hostnames = new string[2];
        if (canaryHost.Rows.Count > 0 && canaryHost.Rows[0]["CanaryName"] != null)
        {
            var dns = string.Empty;

            // ::TODO::
            // var dns = CommonRuntimeConfiguration.DnsSuffix;
            // ::TODO::

            if (string.IsNullOrWhiteSpace(dns)){
                for(int i = 0; i < hostnameSuffix.Length; i++){
                    hostnames[i] = string.Format("{0}.{1}", canaryHost.Rows[0]["CanaryName"], hostnameSuffix[i]);
                }
            }
        }

        return hostnames;
    }


    public static string GetHttpRequestsQuery(string[] hostNames, OperationContext<App> cxt)
    {
        if (hostNames == null || !hostNames.Any())
        {
            throw new ArgumentException("Hostnames or Stampname cannot be null.");
        }
        var flexStampEnablementPredictedDate = new DateTime(2020, 4, 18);
        var currentDate = DateTime.Now;
        if(currentDate < flexStampEnablementPredictedDate)
        {
            return
                $@"set query_results_cache_max_age = time(1d);AntaresIISLogFrontEndTable
                | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)} 
                | where {Utilities.HostNamesFilterQuery(hostNames)}
                | where User_agent != 'AlwaysOn'
                | where User_agent != 'WAWS+CanaryRuntime+V2+(http://www.windowsazure.com/en-us/home/scenarios/web-sites/)'
                | summarize Total=count() by StatusCodeDigit=Sc_status / 100, bin(TIMESTAMP, {TimeGrain}m)
                | order by TIMESTAMP asc ";
        }
        return
                $@"set query_results_cache_max_age = time(1d);AntaresIISLogFrontEndTable
                | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)} 
                | where {Utilities.HostNamesFilterQuery(hostNames)}
                | where User_agent != 'AlwaysOn'
                | summarize Total=count() by StatusCodeDigit=Sc_status / 100, bin(TIMESTAMP, {TimeGrain}m)
                | order by TIMESTAMP asc ";
    }

    public static string GetSiteLatencyQuery(OperationContext<App> cxt, string[] hostNames)
    {
        var query = $@"set query_results_cache_max_age = time(1d);AntaresIISLogFrontEndTable
                |where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)} 
                | {GetSiteAndHostNamesQuery(cxt, false)}
                | where User_agent != 'AlwaysOn' and User_agent != 'HealthCheck/1.0'
                | summarize 50thPercentile = percentile(Time_taken, 50) , 90thPercentile = percentile(Time_taken, 90), 95thPercentile = percentile(Time_taken, 95) by bin(PreciseTimeStamp, {TimeGrain}m)
                | order by PreciseTimeStamp asc";
    
        return query;
    }

    public  static DateTime GetDateTimeInUtcFormat(DateTime dateTime)
    {
        if (dateTime.Kind == DateTimeKind.Unspecified)
        {
            return new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, dateTime.Second, dateTime.Millisecond, DateTimeKind.Utc);
        }

        return dateTime.ToUniversalTime();
    }

    public async static Task<string[]> GetCanarySiteHostName(DataProviders dp, OperationContext<App> cxt)
    {

        /*============== OLD LOGIC TO GET CANARY FROM KUSTO==================================
        var primaryCanaryTask = dp.Kusto.ExecuteClusterQuery(GetCanaryNamePrimaryQuery(cxt), null, "GetCanaryNamePrimaryQuery");
        //var secondaryCanaryTask = dp.Kusto.ExecuteQuery(GetCanaryNameSecondaryQuery(cxt), cxt.Resource.Stamp.Name, null, "GetCanaryNameSecondaryQuery");

        // DataTable canaryHost;
        // try{
        //     canaryHost = await primaryCanaryTask;
        //     if (canaryHost.Rows.Count == 0)
        //     {
        //         canaryHost = await secondaryCanaryTask;
        //     }
        // }catch(Exception){
        //     canaryHost = await secondaryCanaryTask;
        // }

        DataTable canaryHost = await primaryCanaryTask;        
        if (canaryHost.Rows.Count > 0)
        {
            return GetCanaryHostNames(canaryHost);
        }
        else
        {
            return null;
        }
        ============== OLD LOGIC TO GET CANARY FROM KUSTO==================================
        */

        var canaryName = await GetCanaryNameFromObserver(dp, cxt);

        // canary will be empty in case of Shared or Free SKU
        if (!string.IsNullOrWhiteSpace(canaryName))
        {
            DataTable canaryHost = new DataTable();
            canaryHost.Columns.Add("CanaryName");
            var newRow = canaryHost.NewRow();
            newRow["CanaryName"] = canaryName;
            canaryHost.Rows.Add(newRow);
            return GetCanaryHostNames(canaryHost);
        }
        else 
        {
            return null;
        }
    }

    public static async Task<string> GetCanaryNameFromObserver(DataProviders dp, OperationContext<App> cxt)
    {
        string canaryName = string.Empty;
        string ownerName = $"{cxt.Resource.SubscriptionId}+{cxt.Resource.WebSpace}";        
        string sqlQuery = $@"SELECT TOP 1 rs2.SiteName FROM runtime.view_Sites rs INNER JOIN admin.view_WebSites ads ON rs.SiteName = ads.RuntimeSiteName INNER JOIN runtime.view_Sites as rs2 ON rs.ServerFarmId = rs2.ServerFarmId where ads.SiteName = '{cxt.Resource.Name.ToLower()}' and ads.SlotName = '{cxt.Resource.Slot}' and rs.OwnerName = '{ownerName}' and rs2.SiteType = 100";
        var canaryDetails = await dp.Observer.ExecuteSqlQueryAsync(cxt.Resource.Stamp.InternalName, sqlQuery);
        if (canaryDetails.Rows.Count > 0)
        {
            canaryName = canaryDetails.Rows[0]["site_name"].ToString();
        }

        return canaryName;
    }

    public static DataTable FillWithMissingStatusCodes(DataTable requestsTable)
    {
        DataTable statusCodesTable = new DataTable();
        statusCodesTable.Columns.Add("PreciseTimeStamp", typeof(DateTime));
        statusCodesTable.Columns.Add("Http2xx", typeof(double));
        statusCodesTable.Columns.Add("Http3xx", typeof(double));
        statusCodesTable.Columns.Add("Http4xx", typeof(double));
        statusCodesTable.Columns.Add("Http5xx", typeof(double));

        statusCodesTable.PrimaryKey= new DataColumn[] { statusCodesTable.Columns["PreciseTimeStamp"] };
    
        foreach(DataRow dr in requestsTable.Rows)
        {
            var timeStamp = dr["TIMESTAMP"];
            if (int.TryParse(dr["StatusCodeDigit"].ToString(), out int statusCodesDigit) 
                && double.TryParse(dr["Total"].ToString(), out double totalRequests))
            {
                if(statusCodesDigit > 1 && statusCodesDigit <= 5)
                {

                    DataRow existingRow = statusCodesTable.Rows.Find(timeStamp);
                    DataRow newRow = null;
                    if (existingRow != null)
                    {
                        newRow = existingRow;
                    }
                    else
                    {
                        newRow = statusCodesTable.NewRow();
                        newRow["PreciseTimeStamp"] = dr["TIMESTAMP"];
                        statusCodesTable.Rows.Add(newRow);
                    }

                    newRow["Http" + statusCodesDigit + "xx"] = totalRequests; 
                }
                
            }
        }

        foreach(DataRow dr in statusCodesTable.Rows)
        {
            foreach(DataColumn dc in statusCodesTable.Columns)
            {
                if (dc.ColumnName.StartsWith("Http") && dr[dc] == DBNull.Value)
                {
                    dr[dc] = 0;                    
                }
            }
        }
        
        return statusCodesTable;
    }

    public async static Task<DataTable> ExecuteQueryWithRetry(string kustoQuery, DataProviders dp, OperationContext<App> cxt, string queryLabel, int seconds = 18)
    {
        Task<DataTable> retryTask = null;        
        var timeoutTask = Task.Delay(TimeSpan.FromSeconds(seconds));        
        var originalTask = dp.Kusto.ExecuteQuery(kustoQuery, cxt.Resource.Stamp.Name, null, queryLabel);
        await Task.WhenAny(originalTask, timeoutTask);
        
        if(timeoutTask.IsCompleted)
        {
            queryLabel = queryLabel + "Retry";
            retryTask = dp.Kusto.ExecuteQuery(kustoQuery, cxt.Resource.Stamp.Name, null, queryLabel);
        }

        DataTable output = (retryTask == null) ? await originalTask : await (await Task.WhenAny(originalTask, retryTask));
        return output;
    }

    public async static Task<DataTable> PlotRequests(DataProviders dp, OperationContext<App> cxt, Response res)
    {
        DataTable appRequestsSummary = await ExecuteQueryWithRetry(GetHttpRequestsQuery(cxt.Resource.Hostnames.ToArray(), cxt), dp, cxt, "GetHttpRequestsQuery");
        
        var appRequestsTable = FillWithMissingStatusCodes(appRequestsSummary);
        res.Dataset.Add(new DiagnosticData()
        {
            Table = appRequestsTable,
            RenderingProperties = new TimeSeriesRendering()
            {
                GraphOptions = new
                {
                    color = new string[] { "#2ca02c", "#D4E157", "#ad5a10", "#aa0000","#117dbb" },
                    chart = new {
                        height = 300
                    },
                    yAxis = new {
                        title = new {
                            enabled = true,
                            text = "Request Count"
                        }
                    }
                },
                GraphType = TimeSeriesType.StackedAreaGraph
            }
        });

        return appRequestsTable;
    }

    private static DataTable GetAvailability(DataTable appRequestsTable, string availabilityLabel)
    {
        var availabilityTable = new DataTable();
        availabilityTable.Columns.Add("PreciseTimeStamp", typeof(DateTime));
        availabilityTable.Columns.Add(availabilityLabel, typeof(double));

        foreach(DataRow dr in appRequestsTable.Rows)
        {
            if (double.TryParse(dr["Http2xx"].ToString(), out double Http2xx) &&
                double.TryParse(dr["Http3xx"].ToString(), out double Http3xx) &&
                double.TryParse(dr["Http4xx"].ToString(), out double Http4xx) &&
                double.TryParse(dr["Http5xx"].ToString(), out double Http5xx))
            {
                var newRow = availabilityTable.NewRow();
                newRow["PreciseTimeStamp"] = dr["PreciseTimeStamp"];
                
                var successfulRequests = Http2xx + Http3xx + Http4xx;
                var totalRequests = successfulRequests+ Http5xx;
                double availability = totalRequests > 0 ? (successfulRequests / totalRequests * 100) : 100;
                availability = (availability == 0) ? 0.000001 : availability;
                newRow[availabilityLabel] = availability;
                availabilityTable.Rows.Add(newRow);
            }
            
        }

        return availabilityTable;
    }
    
    public async static Task<DataTable> PlotAvailabilityCharts(DataProviders dp, OperationContext<App> cxt, Response res)
    {
        var canaryHostNames = await GetCanarySiteHostName(dp, cxt);
        
        DataTable canaryAvailabilityTable = null;
        
        var appRequestSummaryTask = ExecuteQueryWithRetry(GetHttpRequestsQuery(cxt.Resource.Hostnames.ToArray(), cxt), dp, cxt, "GetHttpRequestsQueryWebAppDown");
        var siteLatencyTask = ExecuteQueryWithRetry(GetSiteLatencyQuery(cxt, cxt.Resource.Hostnames.ToArray()), dp, cxt, "GetSiteLatencyQueryWebAppDown");

        var appRequestsSummary = await appRequestSummaryTask;   
        var appRequestsTable = FillWithMissingStatusCodes(appRequestsSummary);
        DataTable availabilityCombined = GetAvailability(appRequestsTable, AppAvailabilityLabel);

        if (canaryHostNames != null)
        {
            var canaryRequestSummaryTask = ExecuteQueryWithRetry(GetHttpRequestsQuery(canaryHostNames, cxt), dp, cxt, "GetHttpRequestsQuery-Canary");
            var canaryRequestsSummary = await canaryRequestSummaryTask;
            var canaryRequestsTable = FillWithMissingStatusCodes(canaryRequestsSummary);
            canaryAvailabilityTable = GetAvailability(canaryRequestsTable, PlatformAvailabilityLabel);        
            availabilityCombined.Merge(canaryAvailabilityTable);
        }

        List<Tuple<string, bool, Response>> data = new List<Tuple<string, bool, Response>>();

        double minValue = 0;
        double minValuePlat = 0;
        if (canaryAvailabilityTable != null && double.TryParse(availabilityCombined.Compute($"min([{PlatformAvailabilityLabel}])", string.Empty).ToString(), out minValuePlat))
        {
            minValue = minValuePlat;
        }

        if (double.TryParse(availabilityCombined.Compute($"min([{AppAvailabilityLabel}])", string.Empty).ToString(), out double minValueApp))
        {
            minValue = Math.Min(minValueApp, minValuePlat);
        }

        minValue = (minValue >= 99.95) ? 0 : minValue;
        minValue =  minValue - 10;
        minValue = Math.Max(minValue, 0);
        
        var availabilityData = new Response();
        availabilityData.Dataset.Add(new DiagnosticData()
        {
            Table = availabilityCombined,
            RenderingProperties = new TimeSeriesRendering()
            {
                //Title = "Availability",
                GraphOptions = new
                {
                    color = new string[] { "#4ac0f5", "#84bb00" }, 
                    forceY = new double[] {minValue, 100},
                    yAxis = new {
                        tickAmount = 3,
                        ceiling = 100,
                        floor = minValue,
                        startOnTick = true,
                    },
                    chart = new {
                        height = 200
                    }
                },
                GraphType = TimeSeriesType.LineGraph,
                DefaultValue = 100
            }
        });

        var requestsData = new Response();
        requestsData.Dataset.Add(new DiagnosticData()
        {
            Table = appRequestsTable,
            RenderingProperties = new TimeSeriesRendering()
            {
                GraphOptions = new
                {
                    color = new string[] { "#2ca02c", "#D4E157", "#ad5a10", "#aa0000","#117dbb" },
                    chart = new {
                        height = 200
                    },
                    yAxis = new {
                        title = new {
                            enabled = true,
                            text = "Request Count"
                        }
                    }
                },
                GraphType = TimeSeriesType.StackedAreaGraph
            }
        });
        var perfData = new Response();
        perfData.Dataset.Add(new DiagnosticData()
        {
            Table = await siteLatencyTask,
            RenderingProperties = new TimeSeriesRendering()
            {
                //Title = "Performance",
                GraphOptions = new
                {
                    color = new string[] { "#4ac0f5", "#84bb00", "#9b5196", "#ff8c00", "#ff0000" },
                    chart = new {
                        height = 200
                    }
                },
                GraphType = TimeSeriesType.LineGraph
            }
        });

        data.Add(new Tuple<string, bool, Response>("Availability", true, availabilityData));
        data.Add(new Tuple<string, bool, Response>("Requests", false, requestsData));
        data.Add(new Tuple<string, bool, Response>("Performance", false, perfData));

        Dropdown dropdownViewModel = new Dropdown("View", data);
    
        res.AddDropdownView(dropdownViewModel, "Troubleshoot App Performance and Availability");
        return appRequestsTable;
    }

    public static double GetAppSla(DataTable appRequestsTable)
    {
        var totalRequests = appRequestsTable.Rows.Cast<DataRow>().Sum( row => double.Parse(row["Http2xx"].ToString()) + double.Parse(row["Http3xx"].ToString()) + double.Parse(row["Http4xx"].ToString()) + double.Parse(row["Http5xx"].ToString()));
        var failedRequests = appRequestsTable.Rows.Cast<DataRow>().Sum( row => double.Parse(row["Http5xx"].ToString()));

        var sla = totalRequests > 0 ? ((totalRequests - failedRequests)/totalRequests) * 100 : 100;
        return sla;

    }


    public static string DataTableToMarkdown(DataTable dt)
    {
        if (dt.Rows.Count > 0)
        {
            var markDownBuilder = new StringBuilder();
            List<string> columns = new List<string>();
            markDownBuilder.AppendLine(string.Join(" | ", dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName)));
            string columnHeader = new StringBuilder().Insert(0, " --- |", dt.Columns.Count).ToString();
            columnHeader = columnHeader.Substring(1, columnHeader.Length - 1);
            markDownBuilder.AppendLine(columnHeader);
            foreach (DataRow dr in dt.Rows)
            {
                markDownBuilder.AppendLine(string.Join(" | ", dr.ItemArray));
            }
            return markDownBuilder.ToString();
        }
        else
        {
            return string.Empty;
        }
    }

    public static string GetCpuFromKustoQuery(OperationContext<App> cxt, bool useCache=true)
    {
        string query = $@"{(useCache? "set query_results_cache_max_age = time(1d);": "")}let DWASRecords = materialize (StatsDWASWorkerProcessTenMinuteTable
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource, "TIMESTAMP")}
        | where (ApplicationPool =~ '{cxt.Resource.Name}' or ApplicationPool startswith '{cxt.Resource.Name}__')
        | summarize by bin(TIMESTAMP, 10m), RoleInstance, Tenant, ApplicationPool);
        let Tenants = materialize(DWASRecords | summarize makeset(Tenant));
        let RoleInstances = materialize(DWASRecords | summarize makeset(RoleInstance)); 
        DWASRecords
        | join kind=inner
        (
            StatsCounterFiveMinuteTable
            | where {Utilities.TimeFilterQuery(cxt.StartTime, cxt.EndTime, "TIMESTAMP")}
            | where Tenant in (Tenants)
            | where RoleInstance in (RoleInstances)
            | where CounterName == ""\\Processor(_Total)\\% Processor Time""    
            | project TIMESTAMP = bin(TIMESTAMP, 10m), ACTUALTIMESTAMP = TIMESTAMP, Tenant, RoleInstance, CounterName, CounterValue
        )
        on TIMESTAMP, RoleInstance, Tenant
        | project TIMESTAMP = ACTUALTIMESTAMP, Tenant, RoleInstance, CounterName, CounterValue, ApplicationPool
        | summarize CounterValue = avg(CounterValue) by bin(TIMESTAMP, {TimeGrain}m), Tenant, RoleInstance, CounterName, ApplicationPool
        | join kind= leftouter (
            RoleInstanceHeartbeat 
            | where {Utilities.TimeFilterQuery(cxt.StartTime, cxt.EndTime, "TIMESTAMP")}
            | where Tenant in (Tenants)
            | where RoleInstance in (RoleInstances)
            | summarize by RoleInstance, Tenant, MachineName
        ) on RoleInstance, Tenant";

        if (cxt.IsInternalCall)
        {
            query += "| project TIMESTAMP, RoleInstance, Tenant, ApplicationPool, OverallCPUPercent=CounterValue,MachineName=strcat(RoleInstance,'[',MachineName,']') ";
        }
        else
        {
            query += "| project TIMESTAMP, RoleInstance, Tenant, ApplicationPool, OverallCPUPercent=CounterValue,MachineName";
        }

        return query;
    }

    public async static Task<string> GetMdmIdForSiteServerFarm(OperationContext<App> cxt, DataProviders dp)
    {
        string mdmId = string.Empty;
        string ownerName = $"{cxt.Resource.SubscriptionId}+{cxt.Resource.WebSpace}";
        
        string sqlQuery = $@"SELECT TOP 1 rs.SiteId, rs.ServerFarmId, rs.ServerFarmName, rs.SKU FROM runtime.view_Sites rs INNER JOIN admin.view_WebSites ads  ON rs.SiteName = ads.RuntimeSiteName where ads.SiteName = '{cxt.Resource.Name.ToLower()}' and ads.SlotName = '{cxt.Resource.Slot}' and OwnerName = '{ownerName}'";
        var serverFarmDetails = await dp.Observer.ExecuteSqlQueryAsync(cxt.Resource.Stamp.InternalName, sqlQuery);
        if (serverFarmDetails.Rows.Count > 0)
        {
            string sku = serverFarmDetails.Rows[0]["sku"].ToString().ToLower();
            var serverFarmId = serverFarmDetails.Rows[0]["server_farm_id"].ToString();
            if (sku != "shared" && sku != "free" && sku != "dynamic")
            {
                mdmId = $"{cxt.Resource.Stamp.InternalName}_{serverFarmId}";
            }
        }
        return mdmId;
    }

    public async static Task<Tuple<string, string>> GetMdmIdAndSkuForServerFarm(OperationContext<App> cxt, DataProviders dp)
    {
        string mdmId = string.Empty;
        string sku = string.Empty;
        string ownerName = $"{cxt.Resource.SubscriptionId}+{cxt.Resource.WebSpace}";

        string sqlQuery = $@"SELECT TOP 1 rs.SiteId, rs.ServerFarmId, rs.ServerFarmName, rs.SKU FROM runtime.view_Sites rs INNER JOIN admin.view_WebSites ads  ON rs.SiteName = ads.RuntimeSiteName where ads.SiteName = '{cxt.Resource.Name.ToLower()}' and ads.SlotName = '{cxt.Resource.Slot}' and OwnerName = '{ownerName}'";
        var serverFarmDetails = await dp.Observer.ExecuteSqlQueryAsync(cxt.Resource.Stamp.InternalName, sqlQuery);
        if (serverFarmDetails.Rows.Count > 0)
        {
            sku = serverFarmDetails.Rows[0]["sku"].ToString().ToLower();
            var serverFarmId = serverFarmDetails.Rows[0]["server_farm_id"].ToString();
            if (sku != "shared" && sku != "free" && sku != "dynamic")
            {
                mdmId = $"{cxt.Resource.Stamp.InternalName}_{serverFarmId}";
            }
        }
        var result = Tuple.Create(sku, mdmId);
        return result;
    }

    public static async Task<DataTable> GetMdmDataForMetricAppServicePlan(string metric, string mdmId, OperationContext<App> cxt, DataProviders dp, Response res, IEnumerable<string> instances, string metricLabel)
    {
         var dataTable = await GetInstanceLevelMdmData("Microsoft/Web/AppServicePlans", metric, mdmId, cxt, dp, res, instances, metricLabel);
         return dataTable;
    }

    public static async Task<DataTable> GetInstanceLevelMdmData(string ns, string metric, string mdmId, OperationContext<App> cxt, DataProviders dp, Response res, IEnumerable<string> instances, string metricLabel, DateTime? overrideStartDate = null, DateTime? overrideEndDate = null, Func<string, string, double, double> metricProc = null)
    {
        List<DataTable> allInstances = new List<DataTable>();
        DataTable mergedTables = null;
        List<Tuple<string, string, IEnumerable<KeyValuePair<string, string>>>> definitions = new List<Tuple<string, string, IEnumerable<KeyValuePair<string, string>>>>();
              
        foreach (var instance in instances)
        {
            var instanceDimension = new Dictionary<string, string> { { "ResourceId", mdmId }, { "ServerName", instance } };
            var instanceDefintion = Tuple.Create<string, string, IEnumerable<KeyValuePair<string, string>>>(ns, metric, instanceDimension);
            definitions.Add(instanceDefintion);
        }

        if (DateTime.TryParse(cxt.StartTime, out var startTime) && DateTime.TryParse(cxt.EndTime, out var endTime))
        {
            if (overrideStartDate != null)
            {
                startTime = overrideStartDate.Value;
            }

            if (overrideEndDate != null)
            {
                endTime = overrideEndDate.Value;
            }

            var tmp = await dp.Mdm(MdmDataSource.Antares).GetMultipleTimeSeriesAsync(startTime, endTime, Sampling.Average, definitions, 5);
            List<DataTable> allInstancesData = tmp.ToList();
            if (allInstancesData.Count > 0)
            {
                for (int i = 0; i < allInstancesData.Count; i++)
                {
                    string instanceName = instances.ToList()[i];
                    DataTable perInstanceDataTable = new DataTable();
                    perInstanceDataTable.Columns.Add("TIMESTAMP", typeof(DateTime));
                    perInstanceDataTable.Columns.Add("MachineName");
                    perInstanceDataTable.Columns.Add(metricLabel, typeof(double));

                    var table = allInstancesData[i];
                    foreach (DataRow dr in table.Rows)
                    {
                        var newRow = perInstanceDataTable.NewRow();
                        newRow["TIMESTAMP"] = dr["TimeStamp"];
                        newRow["MachineName"] = instanceName;
                        newRow[metricLabel] = metricProc != null ? metricProc(metricLabel, instanceName, (double)dr["Average"]) : dr["Average"];
                        perInstanceDataTable.Rows.Add(newRow);
                    }
                    allInstances.Add(perInstanceDataTable);
                }
            }
        }

        if (allInstances.Count > 0)
        {
            mergedTables = allInstances[0];
            for (int i = 1; i < allInstances.Count; i++)
            {
                mergedTables.Merge(allInstances[i], false, MissingSchemaAction.Add);
            }
        }

        mergedTables = RemoveInstancesWithZeroValue(mergedTables, metricLabel);

        return mergedTables;
    }

    public static DataTable RemoveInstancesWithZeroValue(DataTable dataTable, string metricLabel)
    {
        if (dataTable !=null && dataTable.Rows.Count > 0)
        {
            var instancesWithValues = dataTable.Rows.Cast<DataRow>().Where(row => double.TryParse(row[metricLabel].ToString(), out double metricLabelValue) && metricLabelValue > 0).Select(row => row["MachineName"].ToString()).Distinct();
            
            if (instancesWithValues != null && instancesWithValues.Count() > 0)
            {
                instancesWithValues = instancesWithValues.Select( x=> $"'{x}'").ToList();
                var rows = dataTable.Select($"MachineName NOT in ({ string.Join(",", instancesWithValues) })");
                foreach (var row in rows)
                {
                    row.Delete();
                }
            }
        }
        return dataTable;
    }

    public static async Task<IEnumerable<string>> GetInstancesNamesFromMdm(string ns, string resourceId, string metricName, string dimensionName, OperationContext<App> cxt, DataProviders dp, DateTime? overrideStartDate = null, DateTime? overrideEndDate = null)
    {
        IEnumerable<string> instances = null;
        if (DateTime.TryParse(cxt.StartTime, out var startTime) && DateTime.TryParse(cxt.EndTime, out var endTime))
        {
            if (overrideStartDate != null)
            {
                startTime = overrideStartDate.Value;
            }

            if (overrideEndDate != null)
            {
                endTime = overrideEndDate.Value;
            }
            var filter = new List<Tuple<string, IEnumerable<string>>>
                {
                    new Tuple<string, IEnumerable<string>>("ResourceId", new List<string>(){resourceId})
                };
            instances = await dp.Mdm(MdmDataSource.Antares).GetDimensionValuesAsync(ns, metricName, filter, dimensionName, startTime, endTime);
        }
        return instances;    
    }

    public static async Task<DataTable> GetMdmDataForMetricWebApp(string metric, OperationContext<App> cxt, DataProviders dp, Response res)
    {
        DataTable table = null;
        var ns = "Microsoft/Web/WebApps";
        var dimensions = new Dictionary<string, string> { { "ResourceId", cxt.Resource.DefaultHostName } };
        if (DateTime.TryParse(cxt.StartTime, out var startTime) && DateTime.TryParse(cxt.EndTime, out var endTime))
        {
            var tmp = await dp.Mdm(MdmDataSource.Antares).GetTimeSeriesAsync(startTime, endTime, Sampling.Average, ns, metric, dimensions);
            table = tmp.ToList().FirstOrDefault();
        }
        return table;
    }

    public static async Task AddPerInstanceViewForMdmCounter(OperationContext<App> cxt, DataProviders dp, Response res, string[] counterNameArray, string[] graphTitle, Func<string, string, double, double> metricProc = null)
    {
        var instanceNamesTask = new Dictionary<PerInstanceCounterRendering, Task<IEnumerable<string>>>();

        int i = 0;
        foreach(var counter in counterNameArray)
        {
            var rendering = new PerInstanceCounterRendering()
            {
                Table = null,
                CounterName = counter,
                Title = graphTitle[i]                        
            };

            instanceNamesTask.Add(rendering, GetInstancesNamesFromMdm("Microsoft/Web/WebApps",
                                                                    cxt.Resource.DefaultHostName,
                                                                    counter,
                                                                    "ServerName",
                                                                    cxt,
                                                                    dp));
            i++;
        }

        await Task.WhenAll(instanceNamesTask.Values);

        foreach(KeyValuePair<PerInstanceCounterRendering, Task<IEnumerable<string>>> kvp in instanceNamesTask)
        {
            var instanceNames = await kvp.Value;
            string counterName = kvp.Key.CounterName;
            if (instanceNames.Any())
            {
                kvp.Key.Table = await GetInstanceLevelMdmData("Microsoft/Web/WebApps", 
                                                                counterName, 
                                                                cxt.Resource.DefaultHostName,
                                                                cxt, 
                                                                dp, 
                                                                res, 
                                                                instanceNames, 
                                                                counterName,
                                                                metricProc: metricProc);
                                                                    
            }
        }

        if (instanceNamesTask.Count() == 1)
        {
            var kvp = instanceNamesTask.FirstOrDefault();

            if (kvp.Key.Table != null && kvp.Key.Table.Rows.Count > 0)
            {

                res.Dataset.Add(new DiagnosticData()
                {
                    Table = kvp.Key.Table,
                    RenderingProperties = new TimeSeriesRendering()
                    {
                        Title = kvp.Key.Title,
                        GraphType = TimeSeriesType.LineGraph
                    }
                });
            }
        }
        else if (instanceNamesTask.Count() > 1)
        {
            List<Tuple<string, bool, Response>> dropdownData = new List<Tuple<string, bool, Response>>();
            bool selected = true;
            foreach(KeyValuePair<PerInstanceCounterRendering, Task<IEnumerable<string>>> kvp in instanceNamesTask)
            {
                if (kvp.Key.Table != null)
                {
                    Response perInstanceResponse = new Response();

                    perInstanceResponse.Dataset.Add(new DiagnosticData()
                    {
                        Table = kvp.Key.Table,
                        RenderingProperties = new TimeSeriesRendering()
                        {
                            Title = kvp.Key.Title,
                            GraphType = TimeSeriesType.LineGraph
                        }
                    });

                    dropdownData.Add(new Tuple<string, bool, Response>(
                        kvp.Key.Title,
                        selected,
                        perInstanceResponse));
                        
                    selected = false;    
                }
            }

            if (dropdownData.Any())
            {
                Dropdown dropdown = new Dropdown($"Choose a counter name to get per instance view : ", dropdownData);
                res.AddDropdownView(dropdown, "Per Instance View");
            }
        }        
    }

    public static async Task<StackType> GetApplicationStack(OperationContext<App> cxt, DataProviders dp)
        {
           
            string query =
                $@"set query_results_cache_max_age = time(1d);
                WawsAn_dailyentity
                | where pdate >= ago(5d) and sitename =~ ""{cxt.Resource.Name}"" and sitesubscription =~ ""{cxt.Resource.SubscriptionId}"" and resourcegroup =~ ""{cxt.Resource.ResourceGroup}""
                | where sitestack !has ""unknown"" and sitestack !has ""no traffic"" and sitestack  !has ""undefined""
                | top 1 by pdate desc
                | project sitestack";

            DataTable stackTable = null;

            try
            {
                stackTable = await dp.Kusto.ExecuteClusterQuery(query, "wawseusfollower", "wawsprod", "", operationName: "GetApplicationStack");
            }
            catch (Exception)
            {
                //swallow the exception. Since Mooncake does not have an analytics cluster
                // DiagnosticsETWProvider.Instance.LogRuntimeHostHandledException(dataProviderContext.RequestId, "GetApplicationStack", subscriptionId,
                //     resourceGroup, siteName, ex.GetType().ToString(), ex.ToString());
            }

            if (stackTable == null || stackTable.Rows == null || stackTable.Rows.Count == 0)
            {
                return StackType.None;
            }

            return GetAppStackType(stackTable.Rows[0][0].ToString().ToLower());
        }
        
        //
        //Overloaded version of GetApplicationStack to allow to request stack by specifiying a Web App name different than the one in cxt
        //
        public static async Task<StackType> GetApplicationStack(String SiteName, String SubscriptionId, String ResourceGroup, DataProviders dp)
        {
           
            string query =
                $@"set query_results_cache_max_age = time(1d);
                WawsAn_dailyentity
                | where pdate >= ago(5d) and sitename =~ ""{SiteName}"" and sitesubscription =~ ""{SubscriptionId}"" and resourcegroup =~ ""{ResourceGroup}""
                | where sitestack !has ""unknown"" and sitestack !has ""no traffic"" and sitestack  !has ""undefined""
                | top 1 by pdate desc
                | project sitestack";

            DataTable stackTable = null;

            try
            {
                stackTable = await dp.Kusto.ExecuteClusterQuery(query, "wawseusfollower", "wawsprod", "", operationName: "GetApplicationStack");
            }
            catch (Exception)
            {
                //swallow the exception. Since Mooncake does not have an analytics cluster
                // DiagnosticsETWProvider.Instance.LogRuntimeHostHandledException(dataProviderContext.RequestId, "GetApplicationStack", subscriptionId,
                //     resourceGroup, siteName, ex.GetType().ToString(), ex.ToString());
            }

            if (stackTable == null || stackTable.Rows == null || stackTable.Rows.Count == 0)
            {
                return StackType.None;
            }

            return GetAppStackType(stackTable.Rows[0][0].ToString().ToLower());
        }


        private static StackType GetAppStackType(string stackString)
        {
            switch (stackString)
            {
                case "asp.net":
                case "classic asp":
                case "aspnet":
                    return StackType.AspNet;
                case "asp.net core":
                case "dotnetcore":
                case @"dotnetcore""":
                    return StackType.NetCore;
                case "php":
                    return StackType.Php;
                case "python":
                    return StackType.Python;
                case "java":
                    return StackType.Java;
                case "node":
                    return StackType.Node;
                case "sitecore":
                    return StackType.Sitecore;       
                case "static only":
                case "static":
                    return StackType.Static;
                default:
                    return StackType.Other;
            }
        }

        class PerInstanceCounterRendering
        {
            public DataTable Table {get;set;}
            public string CounterName {get;set;}
            public string Title {get;set;}
        }


    public static string GetInstancesForSiteRuntimeNames(OperationContext<App> cxt)
    {
        return
        $@"set query_results_cache_max_age = time(1d);
        StatsDWASWorkerProcessTenMinuteTable
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource, "TIMESTAMP")}
        | where ApplicationPool =~ '{cxt.Resource.Name}' or ApplicationPool startswith '{cxt.Resource.Name}__'
        | summarize RoleInstances=tostring(makeset(strcat(RoleInstance,';',Tenant))) by ApplicationPool";
    }

    public static IEnumerable<RoleInstanceAndTenant> GetInstancesFromSlotsOnDifferentAppServicePlan(DataTable instancesForSiteRuntimeNames, Dictionary<string, List<RuntimeSitenameTimeRange>> slotTimeRanges, OperationContext<App> cxt, Response res)
    {
        var incorrectInstances = new List<RoleInstanceAndTenant>();
        string[] commonInstances = null;
        var siteRuntimeNames = new Dictionary<string,string[]>();

        // If we get just one site runtime name, then 
        // no need to check anything else.
        if ( instancesForSiteRuntimeNames.Rows.Count < 2)
        {
            return incorrectInstances;
        }

        foreach(DataRow row in instancesForSiteRuntimeNames.Rows)
        {
            var site = row["ApplicationPool"].ToString();
            
            var instances = row["RoleInstances"].ToString().Replace("[","")
                            .Replace("]","")
                            .Replace("\"","").
                            Split(','); 
            
            siteRuntimeNames.Add(site, instances);

            if (commonInstances == null)
            {
                commonInstances = instances;
            }
            else
            {
                commonInstances = instances.Intersect(commonInstances).ToArray();
            }
        }

        // Even if we found one instance which was running for all the
        // slots, we can assume that the slots were on same serverfarm
        if (!commonInstances.Any())
        {
            var allowedSiteRuntimeNames = new List<string>();
            foreach(var slot in slotTimeRanges.Where(slot => slot.Key == cxt.Resource.Slot))
            {
                foreach(var runtimeSiteInfo in slot.Value)
                {
                    var slotName = slot.Key;

                    var slotrangeStartTime = runtimeSiteInfo.StartTime;
                    var slotrangeEndTime = runtimeSiteInfo.EndTime;
                    var siteRuntimeName = runtimeSiteInfo.RuntimeSitename; 
                
                    var startTime = DateTime.ParseExact(cxt.StartTime, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);
                    var endTime = DateTime.ParseExact(cxt.EndTime, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);
                
                    bool overlap = startTime < slotrangeEndTime && slotrangeStartTime < endTime;
                    if (overlap)
                    {
                        if (!allowedSiteRuntimeNames.Contains(siteRuntimeName))
                        {
                            allowedSiteRuntimeNames.Add(siteRuntimeName);
                        }
                    }
                }
            }
            
            //
            // First, identify all the valid instances for the site. This is done by 
            // finding all those instances on which a site with the allowed SiteRuntime
            // name is running.
            //

            var validInstances = new List<string>();
            foreach (var siteRuntimeName in allowedSiteRuntimeNames)
            {
                validInstances = validInstances.Union(siteRuntimeNames[siteRuntimeName]).ToList();
            }

            //
            // Next, filter all those instances that are not a part of 
            // valid instance list. These are all the instances that should
            // be removed from the resultset.
            //

            var instancesToBeRemoved = new List<string>();
            foreach (var item in siteRuntimeNames)
            {
                foreach (var instance in item.Value.Where(x => !validInstances.Contains(x)))
                {
                    if (!instancesToBeRemoved.Contains(instance))
                    {
                        instancesToBeRemoved.Add(instance);
                        var entry = GetRoleInstanceAndTenantFromString(instance);
                        if (entry != null)
                        {
                            incorrectInstances.Add(entry);
                        }
                    }
                }
            }                        
        }
        
        return incorrectInstances;
    }

    private static RoleInstanceAndTenant GetRoleInstanceAndTenantFromString(string instance)
    {
        var instanceArray = instance.Split(";");
        if (instanceArray.Length != 2)
        {
            return null;
        }

        var entry = new RoleInstanceAndTenant()
        {
            RoleInstance = instanceArray[0],
            Tenant = instanceArray[1]
        };
            
        return entry;
    }

    public static DataTable RemoveInstancesFromOtherSlots(DataTable sourceTable,IEnumerable<RoleInstanceAndTenant> instancesNeedingRemoval)
    {
        if (instancesNeedingRemoval != null 
        && instancesNeedingRemoval.Any() 
        && sourceTable != null
        && sourceTable.Rows.Count > 0
        && sourceTable.Columns.Contains("Tenant") 
        && sourceTable.Columns.Contains("RoleInstance"))
        {
            var rowsToDelete = new List<DataRow>();
            foreach(DataRow row in sourceTable.Rows)
            {
                if (instancesNeedingRemoval.Any(x => x.RoleInstance == row["RoleInstance"].ToString() && x.Tenant == row["Tenant"].ToString()))
                {
                    rowsToDelete.Add(row);
                }
            }

            foreach(var dr in rowsToDelete) 
            {     
                sourceTable.Rows.Remove(dr); 
            }
            sourceTable.AcceptChanges();
        }
        return sourceTable;
    }

    public class RoleInstanceAndTenant
    {
        public string RoleInstance {get; set;}
        public string Tenant {get;set;}
    }
}
