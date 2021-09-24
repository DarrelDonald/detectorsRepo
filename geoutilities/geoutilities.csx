using System.Linq;
using Diagnostics.ModelsAndUtils.Models;
using Diagnostics.ModelsAndUtils.Models.ResponseExtensions;
using System.Text;
using System.Data;

//third version

[AppFilter]
[Definition(Id = "geoutilities", Name = "Geo Utilities", Author = "xinjin", Description = "A gist that contains common functions and classes for App Service")]
public static class GeoUtilities
{
    public enum ServerFarmSize
    {
        Small = 0,
        Medium = 1,
        Large = 2
    }

    public class MetricSample
    {
        public DateTime PreciseTime { get; set; }
        public string MetricName { get; set; }

        public int Count { get; set; }

    } //End of Class MetricSample

    public class ServerFarm
    {
        // /subscriptions/{subscription_name}/resourceGroups/{server_farm_resource_group}/providers/Microsoft.Web/serverfarms/{server_farm_name}"
        public string Id {get;set;} 
        public string Name {get;set;}
        public string Kind {get;set;}
        public string MDMId {get;set;}
        public bool PerSiteScaling {get;set;}

        public string ResourceGroup {get;set;}
        public int NumberOfWorkers {get;set;}
        public int WorkerSizeId {get;set;}
        public int NumberOfSites {get;set;}
        public string Location {get;set;}
        public string SKU {get;set;}

        public int ScalingTargetWorkerCount {get;set;}

        public int WorkerCount 
        {
            get 
            {
                if(ScalingTargetWorkerCount > 0)
                    return ScalingTargetWorkerCount;
                
                return NumberOfWorkers;
            }
        }
        
        public string Size 
        {
            get 
            {
                return ((ServerFarmSize)WorkerSizeId).ToString();
            }
        }

        public string Subscription
        {
            get 
            {
                return Id.ToLower().Split(new string[] {"/subscriptions/", "/resourcegroups/"},  StringSplitOptions.None)[1];
            }
        }
        
        // Id from hosting database runtime.view_ServerFarms
        public int ServerFarmId {get;set;}

        public bool IsLinux {get;set;}

        public string ToHtml()
        {
            StringBuilder htmlBuilder = new StringBuilder();
            htmlBuilder.AppendLine($@"<div style=""font-size: small"">");
            htmlBuilder.AppendLine($"<div>Subscription: {Subscription}</div>");
            htmlBuilder.AppendLine($"<div>Resource Group: {ResourceGroup}</div>");
            htmlBuilder.AppendLine($"<div>Name: {Name}</div>");
            htmlBuilder.AppendLine("</div>");
            return htmlBuilder.ToString();            
        }
    } //End of Class ServerFarm

    public class Site 
    {
        public string Id {get;set;} 
        public string Name {get;set;}
        public string Status {get;set;}
        public string Kind {get;set;}
        public string ResourceGroup {get;set;}
        public string Type 
        {
            get
            {
                if(Name.Contains("(") || Name.Contains(")"))
                {
                    return Kind.Replace("app", "app(slot)");
                }
                return Kind;
            }
        }
        public string ServerFarmId {get;set;}
        public string ServerFarm
        {
            get 
            {
                return ServerFarmId.Split('/').Last();
            }
        }

        private string sku;
        public string SKU 
        {
            get {
                return (sku != null) ? sku.ToString() : string.Empty;
            }
            set
            {
                sku = value;
            }
        }    
    } //End of Class Site

    public class VNetConnection 
    {
        public string Id {get; set;}
        public string Name {get; set;}
        public string Location {get; set;}
        public string VNetResourceId {get; set;}
        public bool IsSwift {get;set;}
    } //End of Class VNetConnection

    public class VirtualNetwork
    {
        public string Id {get;set;} 
        public string Name {get;set;}
        public string Location {get;set;}
        public string ProvisioningState {get;set;}
        public string[] AddressSpace {get;set;}
        public List<Subnet> Subnets {get;set;}
    } //End of class VirtualNetwork

    public class Subnet
    {
        public string Id {get;set;} 
        public string Name {get;set;}
        public string ProvisioningState {get;set;}
        public string AddressPrefix {get;set;}
        public List<IpConfiguration> IpConfigurations {get;set;}      
    } //End of class Subnet

    public class IpConfiguration
    {
        public string Id {get;set;} 
    } //End of class IpConfiguration

    public class Route
    {
        public string Id {get;set;} 
        public string Name {get;set;}
        public string Location {get;set;}
        public string StartAddress {get;set;}
        public string EndAddress {get;set;} 
        public string RouteType {get;set;}  
        public bool DenyRoute {get;set;}
    } //End of class Route

    public class HybridConnectionRelay
    {
        public string Id {get;set;} 
        public string Name {get;set;}
        public string Location {get;set;} 
        public string Type {get;set;}       
        public string ServiceBusNamespace {get;set;}
        public string RelayName {get;set;}
        public string RelayArmUri {get;set;}
        public string HostName {get;set;}
        public long Port {get;set;}
        public string ContainerId {get;set;}
        public string ContainerName {get;set;}
    } //End of class HybridConnectionRelay

    public static object GetPropertyFromGeoMasterResponse(GeoMasterResponse geoResponse, string propertyName)
    {
        if (geoResponse.Properties.ContainsKey(propertyName))
            return geoResponse.Properties[propertyName];
        return null;
    } //End of method GetPropertyFromGeoMasterResponse

    public static HybridConnectionRelay GetHybridConnectionRelayFromGeoMasterResponse(GeoMasterResponse geoResponse)
    {
        if(geoResponse is null)
            return null;

        var hc = new HybridConnectionRelay 
        {
            Id = geoResponse.Id,
            Name = geoResponse.Name,
            Location = geoResponse.Location,
            Type = geoResponse.Type,
            ServiceBusNamespace = geoResponse.Properties["serviceBusNamespace"],
            RelayName = geoResponse.Properties["relayName"],
            RelayArmUri = geoResponse.Properties["relayArmUri"],
            HostName = geoResponse.Properties["hostname"],
            Port = geoResponse.Properties["port"]      
        };

        return hc;
    } //End of method GetHybridConnectionRelayFromGeoMasterResponse

    public static List<HybridConnectionRelay> GetHybridConnectionRelayList(GeoMasterResponse[] hybridConnectionRelayArray)
    {
        var resultList = new List<HybridConnectionRelay>();
        if(hybridConnectionRelayArray is null)
            return resultList;

        for(int i=0; i<hybridConnectionRelayArray.Length;i++)
        {
            var hc = GetHybridConnectionRelayFromGeoMasterResponse(hybridConnectionRelayArray[i]);
            if(!(hc is null))
                resultList.Add(hc);
        }

        return resultList.OrderBy(p=>p.Name).ToList<HybridConnectionRelay>();
    } //End of method GetHybridConnectionRelayList

    public static Route GetRouteFromGeoMasterResponse(GeoMasterResponse geoResponse)
    {
        if(geoResponse is null)
            return null;

        var route = new Route 
        {
            Id = geoResponse.Id,
            Name = geoResponse.Name,
            Location = geoResponse.Location,
            StartAddress = geoResponse.Properties["startAddress"],
            EndAddress = geoResponse.Properties["endAddress"],
            RouteType = geoResponse.Properties["routeType"],
            DenyRoute = Convert.ToBoolean(geoResponse.Properties["denyRoute"])            
        };

        return route;
    } //End of method GetRouteFromGeoMasterResponse

    public static List<Route> GetRouteList(GeoMasterResponse[] routeArray)
    {
        var resultList = new List<Route>();
        if(routeArray is null)
            return resultList;

        for(int i=0; i<routeArray.Length;i++)
        {
            var route = GetRouteFromGeoMasterResponse(routeArray[i]);
            if(!(route is null))
                resultList.Add(route);
        }

        return resultList.OrderBy(p=>p.Name).ToList<Route>();
    } //End of method GetRouteList

    public static Site GetSiteFromGeoMasterResponse(GeoMasterResponse geoResponse)
    {
        if(geoResponse is null)
            return null;

        var site = new Site 
        {
            Id = geoResponse.Id,
            //Name = geoResponse.Name,
            Name = geoResponse.Properties["name"],
            Status = geoResponse.Properties["state"],
            Kind = geoResponse.Properties["kind"],
            ServerFarmId = geoResponse.Properties["serverFarmId"],
            SKU = geoResponse.Properties?["sku"]
        };

        return site;

    } //End of method GetSiteFromGeoMasterResponse

    public static List<Site> GetSiteList(GeoMasterResponse[] siteArray)
    {
        var resultList = new List<Site>();
        if(siteArray is null)
            return resultList;

        for(int i=0; i<siteArray.Length;i++)
        {
            var site = GetSiteFromGeoMasterResponse(siteArray[i]);
            if(!(site is null))
                resultList.Add(site);
        }

        return resultList.OrderBy(p=>p.Name).ToList<Site>();
    } //End of method GetSiteList

    public static ServerFarm GetServerFarmFromGeoMasterResponse(GeoMasterResponse geoResponse)
    {
        if(geoResponse is null)
            return null;

        var serverFarm = new ServerFarm
        {
            Id = geoResponse.Id,
            Name = geoResponse.Name,
            Location = geoResponse.Location,
            Kind = geoResponse.Properties["kind"],
            MDMId = geoResponse.Properties["mdmId"],
            PerSiteScaling = Convert.ToBoolean(geoResponse.Properties["perSiteScaling"]),
            ResourceGroup = geoResponse.Properties["resourceGroup"],
            NumberOfWorkers = Convert.ToInt32(geoResponse.Properties["numberOfWorkers"]),
            WorkerSizeId = Convert.ToInt32(geoResponse.Properties["workerSizeId"]),
            NumberOfSites = Convert.ToInt32(geoResponse.Properties["numberOfSites"])          
        };

        return serverFarm;

    } //End of method GetServerFarmFromGeoMasterResponse
    
    public static List<ServerFarm> GetServerFarmList(GeoMasterResponse[] serverFarmArray)
    {
        var resultList = new List<ServerFarm>();

        if(serverFarmArray is null)
        {
            return resultList;
        }

        for(int i = 0; i<serverFarmArray.Length; i++)
        {
            var serverFarm = GetServerFarmFromGeoMasterResponse(serverFarmArray[i]);
            if(!(serverFarm is null))
                resultList.Add(serverFarm);
        }

        return resultList.OrderBy(p=>p.Name).ToList<ServerFarm>();
    } //end of method GetServerFarmList    

    public static VirtualNetwork GetVirtualNetworkFromGeoMasterResponse(GeoMasterResponse geoResponse)
    {
        if(geoResponse is null)
            return null;

        var virtualNetwork = new VirtualNetwork
        {
            Id = geoResponse.Id,
            Name = geoResponse.Name,
            Location = geoResponse.Location,
            ProvisioningState = geoResponse.Properties["provisioningState"],
            AddressSpace = geoResponse.Properties["addressSpace"].addressPrefixes,
            Subnets = GetSubnetList(geoResponse.Properties["subnets"])
        };

        return virtualNetwork;
    } //End of method GetVirtualNetworkFromGeoMasterResponse

    public static IpConfiguration GetIpConfigurationFromGeoMasterResponse(GeoMasterResponse geoResponse)
    {
        if(geoResponse is null)
            return null;
        
        var ipConfiguration = new IpConfiguration
        {
            Id = geoResponse.Id
        };
        return ipConfiguration;
    } //End of method GetIpConfigurationFromGeoMasterResponse

    public static List<IpConfiguration> GetIpConfigurationList(GeoMasterResponse[] ipConfigurationArray)
    {
        var resultList = new List<IpConfiguration>();
        if(ipConfigurationArray is null)
            return resultList;
        
        for(int i=0; i<ipConfigurationArray.Length; i++)
        {
            var ipConfiguration = GetIpConfigurationFromGeoMasterResponse(ipConfigurationArray[i]);
            resultList.Add(ipConfiguration);
        }

        return resultList.OrderBy(p=>p.Id).ToList<IpConfiguration>();
    } //End of method GetIpConfigurationList
    public static Subnet GetSubnetFromGeoMasterResponse(GeoMasterResponse geoResponse)
    {
        if(geoResponse is null)
            return null;

        var subnet = new Subnet
        {
            Id = geoResponse.Id,
            Name = geoResponse.Name,
            ProvisioningState = geoResponse.Properties["provisioningState"],
            AddressPrefix = geoResponse.Properties["addressPrefix"],
            IpConfigurations = GetIpConfigurationList(geoResponse.Properties["ipConfigurations"])
        };

        return subnet;
    } //End of method GetSubnetFromGeoMasterResponse

    public static List<Subnet> GetSubnetList(GeoMasterResponse[] subnetArray)
    {
        var resultList = new List<Subnet>();
        if(subnetArray is null)
            return resultList;

        for(int i=0; i<subnetArray.Length;i++)
        {
            var subnet = GetSubnetFromGeoMasterResponse(subnetArray[i]);
            resultList.Add(subnet);
        }

        return resultList.OrderBy(p=>p.Name).ToList<Subnet>();
    } //End of method GetSubnetList

    public static VNetConnection GetVNetConnectionFromGeoMasterResponse(GeoMasterResponse geoResponse)
    {
        if(geoResponse is null)
            return null;

        VNetConnection vnet = new VNetConnection 
        {
            Id = geoResponse.Id,
            Name = geoResponse.Name,
            Location = geoResponse.Location,
            VNetResourceId = geoResponse.Properties["vnetResourceId"],
            IsSwift = Convert.ToBoolean(geoResponse.Properties["isSwift"])
        };
        return vnet;
    } //End of method GetVNetConnectionFromGeoMasterResponse
    public static List<VNetConnection> GetVNetConnectionList(GeoMasterResponse[] vNetConnectionArray)
    {
        var resultList = new List<VNetConnection>();

        if(vNetConnectionArray == null)
        {
            return resultList;
        }

        for(int i = 0; i<vNetConnectionArray.Length; i++)
        {
            var vnet = GetVNetConnectionFromGeoMasterResponse(vNetConnectionArray[i]);
            resultList.Add(vnet);
        }

        return resultList.OrderBy(p=>p.Name).ToList<VNetConnection>();    
    } //End of method GetVNetConnectionList

    public static async Task<Dictionary<string, string>> GetRegionClusterCollection(DataProviders dp, OperationContext<App> cxt, bool returnCluster = true)
    {
        var endTime = DateTimeToKustoFormat(RoundDownTime(DateTime.UtcNow, new TimeSpan(0, 5, 0)));
        string query = $@"
            set query_results_cache_max_age = time(1d);
            cluster('wawseus').database('wawsprod').WawsAn_regionsincluster
            | where pdate >= ago(5d) and pdate < datetime({endTime})
            | summarize by KustoCluster = ClusterName, Region, LocationName
            | where not (Region == 'BER' and KustoCluster == 'wawsneu')
        ";

        var regionClusterTable = await dp.Kusto.ExecuteQuery(query, cxt.Resource.Stamp.Name, null, "WawsAn_regionsincluster");

        var regionClusterCollection = new Dictionary<string, string>();

        string valueColumn = "KustoCluster";
        if(!returnCluster)
            valueColumn = "LocationName";

        foreach (DataRow row in regionClusterTable.Rows)
        {
            regionClusterCollection.TryAdd(row["Region"].ToString(), row[valueColumn].ToString());
        }

        return regionClusterCollection;
    }

    public static void InitializeRegionClusterNameCollection(Dictionary<string, string> RegionClusterNameCollection)
    {
        RegionClusterNameCollection.Clear();

        RegionClusterNameCollection.Add("AM2", "wawsweu");
        RegionClusterNameCollection.Add("BAY", "wawswus");
        RegionClusterNameCollection.Add("BER", "wawsweu");
        RegionClusterNameCollection.Add("BLU", "wawseus");
        RegionClusterNameCollection.Add("BM1", "wawseas");
        RegionClusterNameCollection.Add("BN1", "wawseus");
        RegionClusterNameCollection.Add("CBR20", "wawseas");
        RegionClusterNameCollection.Add("CBR21", "wawseas");
        RegionClusterNameCollection.Add("CH1", "wawscus");
        RegionClusterNameCollection.Add("CPT20", "wawseas");
        RegionClusterNameCollection.Add("CQ1", "wawscus");
        RegionClusterNameCollection.Add("CW1", "wawsneu");
        RegionClusterNameCollection.Add("CY4", "wawscus");
        RegionClusterNameCollection.Add("DB3", "wawsneu");
        RegionClusterNameCollection.Add("DM1", "wawscus");
        RegionClusterNameCollection.Add("EUAPBN1", "wawseus");
        RegionClusterNameCollection.Add("EUAPDM1", "wawscus");
        RegionClusterNameCollection.Add("FRA", "wawsweu");
        RegionClusterNameCollection.Add("GVA", "wawsweu");
        RegionClusterNameCollection.Add("HK1", "wawseas");
        RegionClusterNameCollection.Add("JNB21", "wawseas");
        RegionClusterNameCollection.Add("KW1", "wawseas");
        RegionClusterNameCollection.Add("LN1", "wawsneu");
        RegionClusterNameCollection.Add("MA1", "wawseas");
        RegionClusterNameCollection.Add("ML1", "wawseas");
        RegionClusterNameCollection.Add("MRS", "wawsweu");
        RegionClusterNameCollection.Add("MWH", "wawswus");
        RegionClusterNameCollection.Add("MSFTBAY", "wawswus");
        RegionClusterNameCollection.Add("MSFTINTBAY", "wawswus");
        RegionClusterNameCollection.Add("MSFTBLU", "wawseus");
        RegionClusterNameCollection.Add("MSFTINTBLU", "wawseus");
        RegionClusterNameCollection.Add("MSFTINTBN1", "wawseus");
        RegionClusterNameCollection.Add("MSFTINTCH1", "wawscus");
        RegionClusterNameCollection.Add("MSFTDB3", "wawsneu");
        RegionClusterNameCollection.Add("MSFTINTDM3", "wawscus");
        RegionClusterNameCollection.Add("MSFTHK1", "wawseas");
        RegionClusterNameCollection.Add("MSFTINTHK1", "wawseas");
        RegionClusterNameCollection.Add("OS1", "wawseas");
        RegionClusterNameCollection.Add("OSL", "wawsweu");
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
        RegionClusterNameCollection.Add("ZRH", "wawsweu");
        RegionClusterNameCollection.Add("BJB", "cnwsbjbmc");
        RegionClusterNameCollection.Add("BJS20", "cnwsbjbmc");
        RegionClusterNameCollection.Add("SHA", "cnwsbjbmc");
        RegionClusterNameCollection.Add("SHA20", "cnwsbjbmc");
    } //End of method InitializeRegionClusterNameCollection

    public static void InitializeRegionFullNameCollection(Dictionary<string, string> RegionFullNameCollection)
    {
        RegionFullNameCollection.Clear();

        RegionFullNameCollection.Add("AM2", "West Europe");
        RegionFullNameCollection.Add("BAY", "West US");
        RegionFullNameCollection.Add("BER", "Germany North");
        RegionFullNameCollection.Add("BLU", "East US");
        RegionFullNameCollection.Add("BM1", "West India");
        RegionFullNameCollection.Add("BN1", "East US 2");
        RegionFullNameCollection.Add("CBR20", "Australia Central");
        RegionFullNameCollection.Add("CBR21", "Australia Central 2");
        RegionFullNameCollection.Add("CH1", "North Central US");
        RegionFullNameCollection.Add("CPT20", "South Africa West");
        RegionFullNameCollection.Add("CQ1", "Brazil South");
        RegionFullNameCollection.Add("CW1", "UK West");
        RegionFullNameCollection.Add("CY4", "West Central US");
        RegionFullNameCollection.Add("DB3", "North Europe");
        RegionFullNameCollection.Add("DM1", "Central US");
        RegionFullNameCollection.Add("FRA", "Germany West Central");
        RegionFullNameCollection.Add("GVA", "Switzerland West");
        RegionFullNameCollection.Add("HK1", "East Asia");
        RegionFullNameCollection.Add("JNB21", "South Africa North");
        RegionFullNameCollection.Add("KW1", "Japan East");
        RegionFullNameCollection.Add("LN1", "UK South");
        RegionFullNameCollection.Add("MA1", "South India");
        RegionFullNameCollection.Add("ML1", "Australia Southeast");
        RegionFullNameCollection.Add("MRS", "France South");
        RegionFullNameCollection.Add("MWH", "West US 2");
        RegionFullNameCollection.Add("OS1", "Japan West");
        RegionFullNameCollection.Add("OSL", "Norway East");
        RegionFullNameCollection.Add("PAR", "France Central");
        RegionFullNameCollection.Add("PN1", "Central India");
        RegionFullNameCollection.Add("PS1", "Korea South");
        RegionFullNameCollection.Add("SE1", "Korea Central");
        RegionFullNameCollection.Add("SG1", "Southeast Asia");
        RegionFullNameCollection.Add("SN1", "South Central US");
        RegionFullNameCollection.Add("SY3", "Australia East");
        RegionFullNameCollection.Add("TY1", "Japan East");
        RegionFullNameCollection.Add("YQ1", "Canada East");
        RegionFullNameCollection.Add("YT1", "Canada Central");
        RegionFullNameCollection.Add("ZRH", "Switzerland North");
    } //End of method InitializeRegionFullNameCollection

    public static string ParseRegionFromStamp(string stampName, bool includePrefix = false)
    {
        if (string.IsNullOrWhiteSpace(stampName))
        {
            throw new ArgumentNullException("stampName");
        }

        var stampParts = stampName.Split(new char[] { '-' });
        if (stampParts.Length >= 3)
        {
            if(includePrefix)
                return stampParts[2];
                
            if(stampParts[2].StartsWith("EUAP", StringComparison.OrdinalIgnoreCase))
                return stampParts[2].Substring(4);
            else if(stampParts[2].StartsWith("MSFT", StringComparison.OrdinalIgnoreCase))
                return stampParts[2].Substring(4);
            else
                return stampParts[2];
        }

        //return * for private stamps if no prod stamps are found
        return "*";
    }//End of method ParseRegionFromStamp

    public static string ShortenInstanceName(string tenantId, string roleInstance)
    {
        //e.g. e3f237bc4abe4a328d425e2a872cf3e2:LargeDedicatedWebWorkerRole_IN_23
        // return e3f237:LDW_23
        var worker = roleInstance.Replace("LargeDedicatedWebWorkerRole_IN_", "LDW_")
                            .Replace("LargeDedicatedLinuxWebWorkerRole_IN_", "LDLW_")
                            .Replace("MediumDedicatedWebWorkerRole_IN_", "MDW_")
                            .Replace("MediumDedicatedLinuxWebWorkerRole_IN_", "MDLW_")
                            .Replace("SmallDedicatedWebWorkerRole_IN_", "SDW_")
                            .Replace("SmallDedicatedLinuxWebWorkerRole_IN_", "SDLW_");

        if(tenantId.StartsWith("waws", StringComparison.OrdinalIgnoreCase))
            return worker;
        else
        {
            var tenant = tenantId.Substring(0, 6);
            return $@"{tenant}:{worker}";
        }

    } //End of method ShortenInstanceName

    /// <summary>
    /// Round down Date time.
    /// </summary>
    /// <param name="dateTime">Date Time to round down.</param>
    /// <param name="roundDownBy">Round down value.</param>
    /// <returns>Rounded down Date Time.</returns>
    public static DateTime RoundDownTime(DateTime dateTime, TimeSpan roundDownBy)
    {
        return new DateTime((dateTime.Ticks / roundDownBy.Ticks) * roundDownBy.Ticks);
    } //End of method RoundDownTime

    public static DateTime RoundUpTime(DateTime dateTime, TimeSpan roundUpBy, int offset = 5)
    {
        TimeSpan offsetSpan = new TimeSpan(0, offset, 0);
        return new DateTime(((dateTime.Ticks + offsetSpan.Ticks) / roundUpBy.Ticks) * roundUpBy.Ticks);
    } //End of method RoundUpTime

    public static DateTime ParseDateTimeUTC(string dt)
    {
        return DateTime.ParseExact(dt, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);
    } //End of method ParseDateTimeUTC

    public static string DateTimeToKustoFormat(DateTime dt)
    {
        return dt.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
    } //End of method DateTimeToKustoFormat

    public static List<MetricSample> EventsToMetricSampleList<T>(List<T> events, string metricProperty, string timeColumn = "PreciseTime")
    {

        return (from dt in events
                group dt by
                new
                {
                    PreciseTime = (DateTime)dt.GetType().GetProperty(timeColumn).GetValue(dt),
                    MetricName = dt.GetType().GetProperty(metricProperty).GetValue(dt).ToString()
                }
        into g
                select
                    new MetricSample { PreciseTime = g.Key.PreciseTime, MetricName = g.Key.MetricName, Count = g.ToList().Count }).OrderBy(p => p.PreciseTime).ToList<MetricSample>();

    } //End of method EventsToMetricSampleList<T>

    public static DataTable MetricSampleListToDataTable(List<MetricSample> eventList)
    {
        var eventTable = new DataTable();

        eventTable.Columns.Add("PreciseTimeStamp", typeof(System.DateTime));
        eventTable.Columns.Add("MetricName");
        eventTable.Columns.Add("Count", typeof(Int32));

        foreach (var item in eventList)
        {
            var currRow = eventTable.NewRow();
            currRow["PreciseTimeStamp"] = item.PreciseTime;
            currRow["MetricName"] = item.MetricName;
            currRow["Count"] = item.Count;
            eventTable.Rows.Add(currRow);

        }

        return eventTable;
    } //End of method MetricSampleListToDataTable

    public static void RemoveColumnsSafely(DataTable tbl, string[] columns)
    {
        var columnsToRemove = new List<DataColumn>();
        for (int i = 0; i < tbl.Columns.Count; i++)
        {
            foreach (string column in columns)
            {
                if (column.ToLower() == tbl.Columns[i].ColumnName.ToLower())
                {
                    if (!columnsToRemove.Contains(tbl.Columns[i]))
                    {
                        columnsToRemove.Add(tbl.Columns[i]);
                    }
                }
            }
        }
        foreach (var column in columnsToRemove)
        {
            tbl.Columns.Remove(column);
        }
    } //End of method RemoveColumnsSafely

} //End of class GeoUtilities
