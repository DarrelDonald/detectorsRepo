using System.Linq;
using Diagnostics.ModelsAndUtils.Models;
using Diagnostics.ModelsAndUtils.Models.ResponseExtensions;
using System.Text;
using System.Data;

[AppFilter]
[Definition(Id = "instanceview", Name = "Instance level table", Author = "xipeng", Description = "A gist that contains common functions for rendering a instance level view ")]
public static class InstanceView
{
    public static IEnumerable<string> AddPortNumberToHostNames(IEnumerable<string> hostnames)
    {
        var hostNamesWithPort = new List<string>();
        foreach(var host in hostnames)
        {
            hostNamesWithPort.Add(host);
            hostNamesWithPort.Add(host+":80");
            hostNamesWithPort.Add(host+":443");
        }

        return hostNamesWithPort;
    }

    public static string GetHttpErrorsCountPerInstancePer5Minutes(OperationContext<App> cxt, string errorType = "5xx")
    {
        //Workaround a known issue related to HostNamesFilterQuery
        var hostnamesQuery = Utilities.HostNamesFilterQuery(AddPortNumberToHostNames(cxt.Resource.Hostnames));
        hostnamesQuery = hostnamesQuery.Replace("\"orCs_host", "\" or Cs_host");
        var errorcodeQuery = errorType == "4xx" ? " Sc_status >=400 and Sc_status < 500" : " Sc_status >=500 and Sc_status < 505";

        return
        $@"AntaresIISLogFrontEndTable 
            | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
            | where {hostnamesQuery}
            | where User_agent != ""AlwaysOn"" and User_agent!=""Azure+Traffic+Manager+Endpoint+Monitor""
            | where {errorcodeQuery}
            | project PreciseTimeStamp, IP = ServerRouted, Sc_status, SiteRuntimeName = S_sitename
            | summarize Count = count() by IP, SiteRuntimeName, bin(PreciseTimeStamp, 5m)
            | join kind=inner (
                RoleInstanceHeartbeat 
                | where  {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)} 
                | extend IP = Details 
                | distinct IP, RoleInstance, Tenant, MachineName 
            ) on IP
            | project PreciseTimeStamp, Tenant, RoleInstance, SiteRuntimeName, Count
            | order by RoleInstance asc, Count desc
            | extend MergedRoleInstance = strcat(substring(Tenant, 0, 6), "":"", iif(RoleInstance startswith ""SmallDedicatedWebWorkerRole_IN_"", replace(""SmallDedicatedWebWorkerRole_IN_"", ""SDW_"", RoleInstance), iif(RoleInstance startswith ""MediumDedicatedWebWorkerRole_IN_"", replace(""MediumDedicatedWebWorkerRole_IN_"", ""MDW_"", RoleInstance),replace(""LargeDedicatedWebWorkerRole_IN_"", ""LDW_"", RoleInstance))))
            | project-away Tenant , RoleInstance 
            | project PreciseTimeStamp, RoleInstance = MergedRoleInstance, Count"; 
    }

    public static string GetErrorsPerInstance(OperationContext<App> cxt, string errorType = "5xx")
    {
        var hostnamesQuery = Utilities.HostNamesFilterQuery(AddPortNumberToHostNames(cxt.Resource.Hostnames));
        hostnamesQuery = hostnamesQuery.Replace("\"orCs_host", "\" or Cs_host");
        var errorcodeQuery = errorType == "4xx" ? " Sc_status >=400 and Sc_status < 500" : " Sc_status >=500 and Sc_status < 505";

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
            | join kind = leftouter (
                AntaresIISLogFrontEndTable 
                | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
                | where {hostnamesQuery}
                | where User_agent != ""AlwaysOn"" and User_agent!=""Azure+Traffic+Manager+Endpoint+Monitor""
                | where {errorcodeQuery}
                | project TIMESTAMP, IP = ServerRouted, Sc_status, SiteRuntimeName = S_sitename 
                | summarize ErrorCount = count() by IP
            ) on IP
            | project Tenant , RoleInstance , ErrorCount = iff(isnull(ErrorCount), 0, ErrorCount) , MachineName, IP, InstanceId
            | order by RoleInstance asc";
    }

    // Build per instance level table
    public static string DataTableToMarkdown(DataTable dt, string stampName, OperationContext<App> cxt, ref int status)
    {
        // Get cluster
        Dictionary<string, string> RegionClusterNameCollection = new Dictionary<string, string>();
        InitializeRegionClusterNameCollection(RegionClusterNameCollection, stampName);
        string regionKey = ParseRegionFromStamp(stampName).ToUpper();
        string cluster = RegionClusterNameCollection[regionKey];

        var markDownBuilder = new StringBuilder();
        List<string> columns = new List<string>();
        markDownBuilder.AppendLine(string.Join(" | ", dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName)));
        string columnHeader = new StringBuilder().Insert(0, " --- |", dt.Columns.Count).ToString();
        columnHeader = columnHeader.Substring(1, columnHeader.Length - 1);
        markDownBuilder.AppendLine(columnHeader);
        foreach (DataRow dr in dt.Rows)
        {
            dr["RoleInstance"] = $@"[{dr["RoleInstance"].ToString()}]({GetWorkerDashboardUrl(dr["Tenant"].ToString(), dr["RoleInstance"].ToString(), cluster, cxt)})";
            if (dr["ErrorCount"].ToString() != "0")
            {
                status = 1;
            }
            markDownBuilder.AppendLine(string.Join(" | ", dr.ItemArray));
        }

        return markDownBuilder.ToString();
    }

    public static string GetWorkerDashboardUrl(string tenant, string roleInstance, string cluster, OperationContext<App> cxt)
    {
        double startMilliseconds = GetMillisecondsFromEpoch(cxt.StartTime);
        double endMilliseconds = GetMillisecondsFromEpoch(cxt.EndTime);

        var result = "https://jarvis-west.dc.ad.msft.net/dashboard/share/688762A6?overrides=[{%22query%22:%22//*[id=%27tenant%27]%22,%22key%22:%22value%22,%22replacement%22:%22" + tenant + "%22},{%22query%22:%22//*[id=%27roleInstance%27]%22,%22key%22:%22value%22,%22replacement%22:%22" + roleInstance + "%22},{%22query%22:%22//*[id=%27cluster%27]%22,%22key%22:%22value%22,%22replacement%22:%22" + cluster + "%22}]&globalStartTime=" + startMilliseconds.ToString() + "&globalEndTime=" + endMilliseconds.ToString() + "&pinGlobalTimeRange=true";

        return result;
    }


    public static double GetMillisecondsFromEpoch(string dt)
    {
        DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var startTime = DateTime.ParseExact(dt, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);
        return (startTime - epoch).TotalMilliseconds;
    }

    public static string ParseRegionFromStamp(string stampName)
    {
        if (string.IsNullOrWhiteSpace(stampName))
        {
            throw new ArgumentNullException("stampName");
        }

        var stampParts = stampName.Split(new char[] { '-' });
        if (stampParts.Length >= 3)
        {
            if (stampParts[2].StartsWith("EUAP", StringComparison.OrdinalIgnoreCase))
                return stampParts[2].Substring(4);
            else if (stampParts[2].StartsWith("MSFT", StringComparison.OrdinalIgnoreCase))
                return stampParts[2].Substring(4);
            else
                return stampParts[2];
        }

        //return * for private stamps if no prod stamps are found
        return "*";
    }


    public static void InitializeRegionClusterNameCollection(Dictionary<string, string> RegionClusterNameCollection, string stampName = null)
    {
        RegionClusterNameCollection.Clear();

        RegionClusterNameCollection.Add("AM2", "wawsweu");
        RegionClusterNameCollection.Add("BAY", "wawswus");
        RegionClusterNameCollection.Add("BLU", "wawseus");
        RegionClusterNameCollection.Add("BM1", "wawseas");
        RegionClusterNameCollection.Add("BN1", string.IsNullOrWhiteSpace(stampName) ? "wawseus" : stampName.StartsWith("waws", StringComparison.CurrentCultureIgnoreCase) ? "wawseus" : "gcwsbn1ff");
        RegionClusterNameCollection.Add("CBR20", "wawseas");
        RegionClusterNameCollection.Add("CBR21", "wawseas");
        RegionClusterNameCollection.Add("JNB21", "wawseas");
        RegionClusterNameCollection.Add("CH1", "wawscus");
        RegionClusterNameCollection.Add("CQ1", "wawscus");
        RegionClusterNameCollection.Add("CW1", "wawsneu");
        RegionClusterNameCollection.Add("CY4", "wawscus");
        RegionClusterNameCollection.Add("DB3", "wawsneu");
        RegionClusterNameCollection.Add("DM1", "wawscus");
        RegionClusterNameCollection.Add("DM3", "wawscus");
        RegionClusterNameCollection.Add("HK1", "wawseas");
        RegionClusterNameCollection.Add("KW1", "wawseas");
        RegionClusterNameCollection.Add("LN1", "wawsneu");
        RegionClusterNameCollection.Add("MA1", "wawseas");
        RegionClusterNameCollection.Add("ML1", "wawseas");
        RegionClusterNameCollection.Add("MRS", "wawsweu");
        RegionClusterNameCollection.Add("MWH", "wawswus");
        RegionClusterNameCollection.Add("OS1", "wawseas");
        RegionClusterNameCollection.Add("PAR", "wawsweu");
        RegionClusterNameCollection.Add("PN1", "wawseas");
        RegionClusterNameCollection.Add("PS1", "wawseas");
        RegionClusterNameCollection.Add("SE1", "wawseas");
        RegionClusterNameCollection.Add("SG1", "wawseas");
        RegionClusterNameCollection.Add("SN1", "wawscus");
        RegionClusterNameCollection.Add("SY3", "wawseas");
        RegionClusterNameCollection.Add("TY1", "wawseas");
        RegionClusterNameCollection.Add("XYZ", "wawscus");
        RegionClusterNameCollection.Add("YQ1", "wawseus");
        RegionClusterNameCollection.Add("YT1", "wawscus");
        RegionClusterNameCollection.Add("BJB", "cnwsbjbmc");
        RegionClusterNameCollection.Add("BJS20", "cnwsbjbmc");
        RegionClusterNameCollection.Add("SHA", "cnwsbjbmc");
        RegionClusterNameCollection.Add("SHA20", "cnwsbjbmc");
        RegionClusterNameCollection.Add("SN5", "gcwsbn1ff");
        RegionClusterNameCollection.Add("PHX20", "gcwsbn1ff");
        RegionClusterNameCollection.Add("DM2", "gcwsbn1ff");
        RegionClusterNameCollection.Add("DD3", "gcwsbn1ff");
        RegionClusterNameCollection.Add("BD3", "gcwsbn1ff");
        RegionClusterNameCollection.Add("AUH", "wawseas");
        RegionClusterNameCollection.Add("FRA", "wawsweu");
        RegionClusterNameCollection.Add("OSL", "wawsweu");
        RegionClusterNameCollection.Add("ZRH", "wawsweu");
        RegionClusterNameCollection.Add("DXB", "wawseas");
    }

    public static string ListToMarkdown<T>(List<T> dt, Response res = null, string markdownTitle = "")
    {
        var markDownBuilder = new StringBuilder();

        if (!dt.Any())
        {
            markDownBuilder.AppendLine(">List is empty.");
            return markDownBuilder.ToString();
        }


        markDownBuilder.AppendLine(string.Join(" | ", dt[0].GetType().GetProperties().Select(c => c.Name)));
        string columnHeader = new StringBuilder().Insert(0, " --- |", dt[0].GetType().GetProperties().Count()).ToString();
        columnHeader = columnHeader.Substring(1, columnHeader.Length - 1);
        markDownBuilder.AppendLine(columnHeader);

        foreach (var item in dt)
        {
            List<string> valueList = new List<string>();
            foreach (var prop in item.GetType().GetProperties())
            {
                string propValue;
                try
                {
                    propValue = prop.GetValue(item).ToString();
                }
                catch
                {
                    propValue = string.Empty;
                }

                valueList.Add(propValue);
            }

            markDownBuilder.AppendLine(string.Join(" | ", valueList));
        }

        if (!(res is null))
            res.AddMarkdownView(markDownBuilder.ToString(), markdownTitle);

        return markDownBuilder.ToString();
    }


    public static string RuntimeSiteSlotMapToMarkdown(Dictionary<string, List<RuntimeSitenameTimeRange>> slotRuntimeMap, Response res = null, string markdownTitle = "")
    {
        var markDownBuilder = new StringBuilder();
        markDownBuilder.AppendLine("| Slot | Site Runtime Name | Start Time | End Time |");
        markDownBuilder.AppendLine("| --- | --- | --- | --- |");

        foreach (KeyValuePair<string, List<RuntimeSitenameTimeRange>> kvp in slotRuntimeMap)
        {
            foreach (var item in kvp.Value)
            {
                markDownBuilder.AppendLine($@"|{kvp.Key}|{item.RuntimeSitename}|{item.StartTime}|{item.EndTime}|");
            }
        }

        if (!(res is null))
            res.AddMarkdownView(markDownBuilder.ToString(), markdownTitle);

        return markDownBuilder.ToString();
    }
}