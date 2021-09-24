
[AppFilter]
[Definition(Id = "SitesSqlCommands", Name = "Sites Sql Commands", Author = "nmallick", Category="Utilities", Description = "Frequently used sql commands to fetch site properties")]
public static class SitesSqlUtility {
    /*
    =========================
    Use as follows from within a detector
    DataTable alwaysOnSQLQueryResult = await dp.Observer.ExecuteSqlQueryAsync(cxt.Resource.Stamp.Name, SitesSqlUtility.GetAlwaysOnQuery(cxt));
    =========================
    
    *** Note : There is a chance that the values returned by the database may be null / empty. Always check the returned value for DBNull.Value before using.

    */

    public static string GetSiteObjectQuery(OperationContext<App> cxt)
    {
        //returns the following properties raw as they are in the DB. The DB may contain null / empty values. Check for DBNull.Value before using
        /*
        select TOP 1 ads.Id, ads.SubscriptionName, ads.WebSpaceName, ads.ResourceGroupName, ads.SiteName, ads.WebSystemName, ads.RuntimeSiteName, ads.StorageLimit, ads.StorageUsage, ads.Path, ads.SiteMode, ads.ScmType, ads.Created, ads.WebSystemId, ads.SlotName, ads.DeploymentId, ads.AutoSwapSlotName, ads.TargetSwapSlot, ads.SerializedSlotSwapStatus, rs.SiteId, rs.ServerFarmId, rs.ServerFarmName, rs.SKU, rs.VirtualFarmId, rs.VirtualFarmName, rs.OwnerId, rs.OwnerName, rs.ResourceGroup, rs.IsolationMode, rs.RootDirectory, rs.NumberOfWorkers, rs.MaximumDiskSpace, rs.ClrVersion, rs.ClassicPipelineMode, rs.RunningMode, rs.LastModifiedTime, rs.RoutingLastModifiedTime, rs.HandlerNames, rs.ModuleNames, rs.ResourceLimit, rs.SiteType, rs.WebRootSubPath, rs.Options, rs.Weight, rs.RequestTracingEnabled, rs.RequestTracingExpirationTime, rs.DetailedErrorLoggingExpirationTime, rs.HttpLoggingEnabled, rs.AzureMonitorLogCategories, rs.Enabled, rs.AdminEnabled, rs.IsOverQuota, rs.NextQuotaResetTime, rs.PolicyId, rs.ComputeMode, rs.LogsDirectorySizeLimit, rs.StorageVolumeName, rs.StorageVolumeRootPath, rs.SandboxType, rs.WebSocketsEnabled, rs.AlwaysOn, rs.StorageRecoveryDefaultRunningMode, rs.RuntimeAvailabilityState, rs.RemoteDebuggingExpirationTime, rs.JavaVersion, rs.VNETName, rs.GatewaySiteName, rs.SiteAuthFeatureEnabled, rs.SiteAuthEnabled, rs.SiteAuthClientId, rs.SiteAuthAutoProvisioned, rs.CorsSettings, rs.PushSettings, rs.LocalMySqlEnabled, rs.ApiDefinitionUrl, rs.ApiManagementConfig, rs.ClientAffinityEnabled, rs.ClientCertEnabled, rs.ClientCertExclusionPaths, rs.HealthCheckPath, rs.DomainVerificationIdentifiers, rs.HostNamesDisabled, rs.ScalingDisabled, rs.Triggers, rs.HasHttpTrigger, rs.HttpScaleV2Enabled, rs.Kind, rs.HomeStamp, rs.TriggersLastModifiedTime, rs.FunctionContainerMemoryLimitInMB, rs.FunctionContainerCpuPercentage, rs.LegacyStoragePath, rs.NodeVersion, rs.PhpVersion, rs.AppCommandLine, rs.DailyMemoryTimeQuota, rs.SuspendedTill, rs.SiteDisabledReason, rs.DynamicQuotaStartTime, rs.DynamicQuotaLastFunctionExecutionUnits, rs.FunctionExecutionUnitsCache, rs.ConnectionStringInfo, rs.LinuxFxVersion, rs.WindowsFxVersion, rs.ManagedServiceIdentityId, rs.XManagedServiceIdentityId, rs.IsSpot, rs.SpotExpirationTime, rs.HttpsOnly, rs.LastNotifyFullTime, rs.Http20Enabled, rs.MinTLSVersion, rs.FtpsState, rs.FrontEndIpFiltering, rs.FrontEndScmIpFiltering, rs.FrontEndScmIpFilteringUseMain, rs.ReservedInstanceCount, rs.DynamicScalingEnabled, rs.KeyVaultSecretReferences, rs.RedundancyMode, rs.PlaceholderId, rs.FileChangeAuditEnabled, rs.PrivateLinkIdentifiers, rs.BuildVersion  from runtime.view_Sites rs INNER JOIN admin.view_WebSites ads ON rs.SiteName = ads.RuntimeSiteName where ads.SiteName = 'nmallick1' and ads.SlotName = 'production'	
        */
        //return $@"select TOP 1  * from runtime.view_Sites rs INNER JOIN admin.view_WebSites ads ON rs.SiteName = ads.RuntimeSiteName where ads.SiteName = '{cxt.Resource.Name.ToLower()}' and ads.SlotName = '{cxt.Resource.Slot.ToLower()}'";
        return $@"select TOP 1 ads.Id, ads.SubscriptionName, ads.WebSpaceName, ads.ResourceGroupName, ads.SiteName, ads.WebSystemName, ads.RuntimeSiteName, ads.StorageLimit, ads.StorageUsage, ads.Path, ads.SiteMode, ads.ScmType, ads.Created, ads.WebSystemId, ads.SlotName, ads.DeploymentId, ads.AutoSwapSlotName, ads.TargetSwapSlot, ads.SerializedSlotSwapStatus, rs.SiteId, rs.ServerFarmId, rs.ServerFarmName, rs.SKU, rs.VirtualFarmId, rs.VirtualFarmName, rs.OwnerId, rs.OwnerName, rs.ResourceGroup, rs.IsolationMode, rs.RootDirectory, rs.NumberOfWorkers, rs.MaximumDiskSpace, rs.ClrVersion, rs.ClassicPipelineMode, rs.RunningMode, rs.LastModifiedTime, rs.RoutingLastModifiedTime, rs.HandlerNames, rs.ModuleNames, rs.ResourceLimit, rs.SiteType, rs.WebRootSubPath, rs.Options, rs.Weight, rs.RequestTracingEnabled, rs.RequestTracingExpirationTime, rs.DetailedErrorLoggingExpirationTime, rs.HttpLoggingEnabled, rs.AzureMonitorLogCategories, rs.Enabled, rs.AdminEnabled, rs.IsOverQuota, rs.NextQuotaResetTime, rs.PolicyId, rs.ComputeMode, rs.LogsDirectorySizeLimit, rs.StorageVolumeName, rs.StorageVolumeRootPath, rs.SandboxType, rs.WebSocketsEnabled, rs.AlwaysOn, rs.StorageRecoveryDefaultRunningMode, rs.RuntimeAvailabilityState, rs.RemoteDebuggingExpirationTime, rs.JavaVersion, rs.VNETName, rs.GatewaySiteName, rs.SiteAuthFeatureEnabled, rs.SiteAuthEnabled, rs.SiteAuthClientId, rs.SiteAuthAutoProvisioned, rs.CorsSettings, rs.PushSettings, rs.LocalMySqlEnabled, rs.ApiDefinitionUrl, rs.ApiManagementConfig, rs.ClientAffinityEnabled, rs.ClientCertEnabled, rs.ClientCertExclusionPaths, rs.HealthCheckPath, rs.DomainVerificationIdentifiers, rs.HostNamesDisabled, rs.ScalingDisabled, rs.Triggers, rs.HasHttpTrigger, rs.HttpScaleV2Enabled, rs.Kind, rs.HomeStamp, rs.TriggersLastModifiedTime, rs.FunctionContainerMemoryLimitInMB, rs.FunctionContainerCpuPercentage, rs.LegacyStoragePath, rs.NodeVersion, rs.PhpVersion, rs.AppCommandLine, rs.DailyMemoryTimeQuota, rs.SuspendedTill, rs.SiteDisabledReason, rs.DynamicQuotaStartTime, rs.DynamicQuotaLastFunctionExecutionUnits, rs.FunctionExecutionUnitsCache, rs.ConnectionStringInfo, rs.LinuxFxVersion, rs.WindowsFxVersion, rs.ManagedServiceIdentityId, rs.XManagedServiceIdentityId, rs.IsSpot, rs.SpotExpirationTime, rs.HttpsOnly, rs.LastNotifyFullTime, rs.Http20Enabled, rs.MinTLSVersion, rs.FtpsState, rs.FrontEndIpFiltering, rs.FrontEndScmIpFiltering, rs.FrontEndScmIpFilteringUseMain, rs.ReservedInstanceCount, rs.DynamicScalingEnabled, rs.KeyVaultSecretReferences, rs.RedundancyMode, rs.PlaceholderId, rs.FileChangeAuditEnabled, rs.PrivateLinkIdentifiers, rs.BuildVersion  from runtime.view_Sites rs INNER JOIN admin.view_WebSites ads ON rs.SiteName = ads.RuntimeSiteName where Cast(ads.SiteName AS VARCHAR) = Cast('{cxt.Resource.Name.ToLower()}' AS VARCHAR) and CAST(ads.SlotName AS VARCHAR) = CAST('{cxt.Resource.Slot.ToLower()}' AS VARCHAR)";
    }

    public static string GetAlwaysOnQuery(OperationContext<App> cxt)    
    {
        return $@"select TOP 1 IIF(rs.AlwaysOn IS NULL , CAST(0 as BIT), rs.AlwaysOn) as AlwaysOn from runtime.view_Sites rs INNER JOIN admin.view_WebSites ads ON rs.SiteName = ads.RuntimeSiteName where Cast(ads.SiteName AS VARCHAR) = Cast('{cxt.Resource.Name.ToLower()}' AS VARCHAR) and CAST(ads.SlotName AS VARCHAR) = CAST('{cxt.Resource.Slot.ToLower()}' AS VARCHAR)";
    }


    public static string GetSiteSkuQuery(OperationContext<App> cxt)
    {
        return $"select TOP 1 rs.SKU from runtime.view_Sites rs INNER JOIN admin.view_WebSites ads  ON rs.SiteName = ads.RuntimeSiteName where Cast(ads.SiteName AS VARCHAR) = Cast('{cxt.Resource.Name.ToLower()}' AS VARCHAR) and CAST(ads.SlotName AS VARCHAR) = CAST('{cxt.Resource.Slot}' AS VARCHAR)";
    }

    public static string GetSiteRuntimeNameQuery(OperationContext<App> cxt)
    {
        return $"select TOP 1 ads.RuntimeSiteName from runtime.view_Sites rs INNER JOIN admin.view_WebSites ads  ON rs.SiteName = ads.RuntimeSiteName where Cast(ads.SiteName AS VARCHAR) = Cast('{cxt.Resource.Name.ToLower()}' AS VARCHAR) and CAST(ads.SlotName AS VARCHAR) = CAST('{cxt.Resource.Slot}' AS VARCHAR)";
    }

    public static string GetServerFarmInfoQuery(OperationContext<App> cxt)
    {
        return $@"select TOP 1 rs.ServerFarmName, rs.ServerFarmId, rs.VirtualFarmId, rsf.SKU, IIF(rsf.IsOverQuota IS NULL , CAST(0 as BIT), rsf.IsOverQuota) as IsOverQuota, rsf.LastModifiedTime, rsf.NextQuotaResetTime, IIF(rsf.PerSiteScaling IS NULL , CAST(0 as BIT), rsf.PerSiteScaling) as PerSiteScaling, rsf.ScalingTargetWorkerCount, rsf.ScalingTargetWorkerSizeId, IIF(rsf.IsLinux IS NULL , CAST(0 as BIT), rsf.IsLinux) as IsLinux, IIF(rsf.IsXenon IS NULL , CAST(0 as BIT), rsf.IsXenon) as IsXenon, IIF(rsf.IsSpot IS NULL , CAST(0 as BIT), rsf.IsSpot) as IsSpot, rsf.SpotExpirationTime, IIF(rsf.ElasticScaleEnabled IS NULL , CAST(0 as BIT), rsf.ElasticScaleEnabled) as ElasticScaleEnabled, IIF(rsf.EnableEventGrid IS NULL , CAST(0 as BIT), rsf.EnableEventGrid) as EnableEventGrid, rsf.MaximumElasticWorkerCount, rsf.HomeStamp, IIF(rsf.AZBalancing IS NULL , CAST(0 as BIT), rsf.AZBalancing) as AZBalancing  from runtime.view_Sites rs INNER JOIN admin.view_WebSites ads  ON rs.SiteName = ads.RuntimeSiteName INNER JOIN runtime.view_ServerFarms rsf ON rs.ServerFarmId = rsf.ServerFarmId where Cast(ads.SiteName AS VARCHAR) = Cast('{cxt.Resource.Name.ToLower()}' AS VARCHAR) and CAST(ads.SlotName AS VARCHAR) = CAST('{cxt.Resource.Slot}' AS VARCHAR)";
    }

    public static string GetServerFarmNameQuery(OperationContext<App> cxt)
    {
        return $"select TOP 1 rs.ServerFarmName, rs.ServerFarmId, rs.VirtualFarmId from runtime.view_Sites rs INNER JOIN admin.view_WebSites ads  ON rs.SiteName = ads.RuntimeSiteName where Cast(ads.SiteName AS VARCHAR) = Cast('{cxt.Resource.Name.ToLower()}' AS VARCHAR) and CAST(ads.SlotName AS VARCHAR) = CAST('{cxt.Resource.Slot}' AS VARCHAR)";
    }

    public static string GetSitesInServerFarmQuery(OperationContext<App> cxt, long serverFarmId, long virtualFarmId )
    {
        //Execute GetServerFarmNameQuery to get the values required by this method and then call this method
        return $"select distinct SiteName FROM runtime.view_Sites where ServerFarmId = {serverFarmId.ToString()} and VirtualFarmId = {virtualFarmId.ToString()} ";
    }


    public static string GetVirtualFarmInfoQuery(OperationContext<App> cxt, long serverFarmId, long virtualFarmId )
    {
        //Execute GetServerFarmNameQuery to get the values required by this method and then call this method
        return $"SELECT VirtualFarmId, VirtualFarmName, ServerFarmId, ServerFarmName, SKU, MaximumSiteWeight, ExclusiveAssignment, OwnerId, OwnerName, Status, CurrentNumberOfWorkers, TargetNumberOfWorkers, PendingStateTime, TargetWorkerSize, CurrentWorkerSize, ActualNumberOfWorkers, IsSpot, SpotExpirationTime, RunningWorkers, IsLinux FROM runtime.view_VirtualFarms where ServerFarmId = {serverFarmId.ToString()} and VirtualFarmId = {virtualFarmId.ToString()} ";
    }

    public static string GetSiteHostnamesQuery(OperationContext<App> cxt)
    { 
        //return $"select h.HostName, h.HostNameType, h.ChangeId, h.Thumbprint, h.IPBasedSSLState, h.SSLEnabled, h.IpBasedSSLResult, h.VipMappingId from runtime.view_Sites as rs INNER JOIN admin.view_WebSites as ads  ON rs.SiteName = ads.RuntimeSiteName INNER JOIN  runtime.view_HostNames as h ON h.SiteId = rs.SiteId and h.SiteName = ads.RuntimeSiteName where ads.SiteName = '{cxt.Resource.Name}' and ads.SlotName = '{cxt.Resource.Slot}'";
        return $"select h.HostName as HostName, h.HostNameType as HostNameType, h.ChangeId as ChangeId, h.Thumbprint as Thumbprint, h.IPBasedSSLState as IPBasedSSLState, h.SSLEnabled as SSLEnabled, h.IpBasedSSLResult as IpBasedSSLResult, v.VirtualIP as VirtualIP, v.InternalHttpPort as InternalHttpPort, v.InternalHttpsPort as InternalHttpsPort  from runtime.view_Sites as rs INNER JOIN admin.view_WebSites as ads  ON rs.SiteName = ads.RuntimeSiteName INNER JOIN  runtime.view_HostNames as h ON h.SiteId = rs.SiteId and h.SiteName = ads.RuntimeSiteName LEFT JOIN runtime.view_VipMappings as v ON h.VipMappingId = v.VipMappingId where Cast(ads.SiteName AS VARCHAR) = Cast('{cxt.Resource.Name.ToLower()}' AS VARCHAR) and CAST(ads.SlotName AS VARCHAR) = CAST('{cxt.Resource.Slot}' AS VARCHAR)";
    }


    public static string GetSiteWebspaceNameQuery(OperationContext<App> cxt)
    {
        return $"select Top 1 WebSpaceName from admin.view_WebSites where Cast(SiteName AS VARCHAR) = Cast('{cxt.Resource.Name.ToLower()}' AS VARCHAR) and Cast(SlotName AS VARCHAR) = Cast('{cxt.Resource.Slot}' AS VARCHAR)";
    }

    public static string GetSiteWorkers(OperationContext<App> cxt)
    {
        return $@"Select ads.SiteName, rs.WorkerName as IPAddress, rs.InstanceName, rs.DeploymentId as TenantId, rs.VirtualFarmName as ServerFarmName from admin.view_WebSites ads INNER JOIN runtime.view_RunningSites rs On rs.SiteName = ads.RuntimeSiteName where Cast(ads.SiteName AS VARCHAR) = Cast('{cxt.Resource.Name.ToLower()}' AS VARCHAR) and Cast(ads.SlotName AS VARCHAR) = Cast('{cxt.Resource.Slot}' AS VARCHAR)";
    }

    public static string GetSiteTipRules(OperationContext<App> cxt)
    {
        return $@"SELECT * FROM runtime.view_SiteTipRules WHERE Cast(SiteName AS VARCHAR)=CAST('{cxt.Resource.Name.ToLower()}' AS VARCHAR) OR Cast(SiteName AS VARCHAR) like Cast('{cxt.Resource.Name.ToLower()}[__]%' AS VARCHAR)";
    }

    public static string GetSiteNameAndSlotFromHostName(string hostName)
    {
        return $@"SELECT TOP 1 ws.Id,  ws.SiteName, ws.SlotName, hn.Name FROM admin.view_WebSites ws INNER JOIN admin.view_HostNames hn on ws.Id = hn.WebSiteId where Cast(hn.Name AS VARCHAR)=Cast('{hostName}' AS VARCHAR)";
    }

    public static string GetAllSlotsForSite(OperationContext<App> cxt)
    {
        return $@"SELECT distinct SiteName, SlotName FROM admin.view_WebSites where CAST(SiteName AS VARCHAR) = CAST('{cxt.Resource.Name.ToLower()}' AS VARCHAR)";
    }

    public static string GetAllSlotsForSite(OperationContext<App> cxt, string siteName)
    {
        if(string.IsNullOrWhiteSpace(siteName))
        {
            throw new ArgumentNullException("siteName");
        }
        return $@"SELECT distinct SiteName, SlotName FROM admin.view_WebSites where CAST(SiteName AS VARCHAR) = CAST('{siteName.ToLower()}' AS VARCHAR) and SubscriptionName = '{cxt.Resource.SubscriptionId}'";
    }

    public static string GetSiteResourceGroup(OperationContext<App> cxt, string siteName)
    {
        //Can be used to construct ARM resource URI of a site that is a part of the same subscription as the current site, e.g a site that is part of the same ASP but sits in a different RG.
        if(string.IsNullOrWhiteSpace(siteName))
        {
            throw new ArgumentNullException("siteName");
        }

        return $@"SELECT distinct ResourceGroupName FROM admin.view_WebSites where CAST(SiteName AS VARCHAR) = CAST('{siteName.ToLower()}' AS VARCHAR) and SubscriptionName = '{cxt.Resource.SubscriptionId}'";
    }

    public static string GetStampOutboundIpAddress()
    {
        return $@"SELECT ConfigurationValue FROM runtime.view_HostingConfigurations WHERE ConfigurationKey like '%OutboundIpAddresses%' and ConfigurationValue != ''";
    }

    public static string GetStampInboundIpAddress()
    {
        return $@"SELECT ConfigurationValue FROM runtime.view_HostingConfigurations WHERE ConfigurationKey like '%InboundIpAddress%' and ConfigurationValue != ''";
    }

    public static string GetMdmIdAndSkuForServerFarmQuery(OperationContext<App> cxt)
    {
        string ownerName = $"{cxt.Resource.SubscriptionId}+{cxt.Resource.WebSpace}";
        return $@"SELECT TOP 1 rs.SiteId, rs.ServerFarmId, rs.ServerFarmName, rs.SKU FROM runtime.view_Sites rs INNER JOIN admin.view_WebSites ads ON rs.SiteName = ads.RuntimeSiteName where CAST(ads.SiteName AS VARCHAR) = CAST('{cxt.Resource.Name.ToLower()}' AS VARCHAR) and CAST(ads.SlotName AS VARCHAR) = CAST('{cxt.Resource.Slot}' AS VARCHAR) and CAST(OwnerName AS VARCHAR) = CAST('{ownerName}' AS VARCHAR)";
    }

    public static string IsStampVMSSQuery()
    {
       return $@"SELECT TOP 1 ConfigurationValue = IIF(ConfigurationValue IS NULL, CAST(0 as BIT), ConfigurationValue ) FROM runtime.view_HostingConfigurations where ConfigurationKey = 'VmssScaleSkuEnabled'"; 
    }

    public static string GetSiteSettingHistory(OperationContext<App> cxt)
    {
        return $@"select Top 100 AuditId, TimeStampUtc, TransactionId, EventType, SiteId, SiteName, AlwaysOn, OwnerId, OwnerName, IsolationMode, RootDirectory, NumberOfWorkers, MaximumDiskSpace, ClrVersion, ClassicPipelineMode, RunningMode, LastModifiedTime, RoutingLastModifiedTime, HandlerNames, ModuleNames, ResourceLimit, SiteType, WebRootSubPath, Options, VirtualFarmId, ServerFarmId, Weight, RequestTracingEnabled, RequestTracingExpirationTime, DetailedErrorLoggingExpirationTime, HttpLoggingEnabled, AzureMonitorLogCategories, AcrUseManagedIdentityCreds, AcrUserManagedIdentityID, Enabled, AdminEnabled, PolicyId, LogsDirectorySizeLimit, StorageVolumeName, SandboxType, WebSocketsEnabled,  StorageRecoveryDefaultRunningMode, RuntimeAvailabilityState, RemoteDebuggingExpirationTime, JavaVersion, VNETName, DomainVerificationIdentifiers, HostNamesDisabled, ScalingDisabled, Triggers, Kind, HomeStamp, FunctionContainerPolicyId, LegacyStoragePath, NodeVersion, PhpVersion, AppCommandLine, DailyMemoryTimeQuota, SuspendedTill, SiteDisabledReason, DynamicQuotaStartTime, DynamicQuotaLastFunctionExecutionUnits, FunctionExecutionUnitsCache, ConnectionStringInfo, LinuxFxVersion, WindowsFxVersion, ManagedServiceIdentityId, DynamicScalingEnabled, BuildVersion FROM runtime.view_Sites_Audit where Cast(SiteName AS VARCHAR) =  Cast('{cxt.Resource.Name.ToLower()}' AS VARCHAR) or Cast(SiteName AS VARCHAR) like Cast('{cxt.Resource.Name.ToLower()}[__]%' AS VARCHAR) order by TimeStampUtc asc";
    }
}
