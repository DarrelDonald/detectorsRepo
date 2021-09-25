#load "instanceview"
#load "webappdownwindows"
#load "DetectorUtils"

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
using System.ComponentModel;
using System.Linq;
using System.Text;

static Dictionary<string, string> InitializeErrors(OperationContext<App> cxt, string sku)
{
    /*
    =======================================================
    Generated using below powershell, saving for future
    =======================================================
    $userfile =  [xml] (Get-Content c:\winerror.xml)
    $allcodes = $userfile.SelectNodes("/error/httperror/error/httpsubStatus/error")
    foreach($n in $allcodes)
    {
    "{ """ + $n.ParentNode.ParentNode.id + "." + $n.id + """ , """ + $n.description + """},"
    }
    */
    bool isInternal = cxt.IsInternalCall;
    var message403_67 = "Site Disabled (quota enforcement or stopped)" + (isInternal ? GetQuotaViolationMessage(cxt, sku) : "");

    var errorCodes = new Dictionary<string, string>()
    {
        { "400.0" , "Bad Request. The request could not be understood by the server due to malformed syntax. The client should not repeat the request without modifications."},
        { "400.1" , "Invalid Destination Header"},
        { "400.2" , "Invalid Depth Header"},
        { "400.3" , "Invalid If Header"},
        { "400.4" , "Invalid Overwrite Header"},
        { "400.5" , "Invalid Translate Header"},
        { "400.6" , "Invalid Request Body"},
        { "400.7" , "Invalid Content Length"},
        { "400.8" , "Invalid Timeout"},
        { "400.9" , "Invalid Lock Token"},
        { "400.10" , "Invalid XFF Header"},
        { "400.11" , "Invalid Websocket Request"},
        { "400.601" , "Bad client request (ARR)"},
        { "400.602" , "Invalid Time Format (ARR)"},
        { "400.603" , "Parse Range Error(ARR)"},
        { "400.604" , "Client Gone (The client that originated the request seems to have closed the connection before the response was received!)"},
        { "400.605" , "Maximum number of forwards (ARR)"},
        { "400.606" , "Asynchronous competition error (ARR)"},
        { "401.1" , "Logon failed. Typically this means that you have configured authentication and the request came with incorrect credentials."},
        { "401.2" , "Logon failed due to server configuration. Typically this error means that you have configured authentication and the request arrived with no credentials."},
        { "401.3" , "Unauthorized due to ACL on resource"},
        { "401.4" , "Authorization failed by filter"},
        { "401.5" , "Authorization failed by ISAPI/CGI application"},
        { "401.7" , "Url Auth Policy"},
        { "401.501" , "[Dynamic IP Restriction] Deny by concurrent requests"},
        { "401.502" , "[Dynamic IP Restriction] Deny by request rate"},
        { "401.503" , "[IP Restriction] Deny by IP address"},
        { "401.504" , "[IP Restriction] Deny by host name"},
        { "403.0" , "Forbidden"},
        { "403.1" , "Execute access Forbidden"},
        { "403.2" , "Read access forbidden "},
        { "403.3" , "Write access forbidden"},
        { "403.4" , "SSL Required"},
        { "403.5" , "SSL 128 required"},
        { "403.6" , "IP address rejected"},
        { "403.7" , "Client certificate required"},
        { "403.8" , "Site access denied"},
        { "403.9" , "Forbidden: Too many clients are trying to connect to the web server"},
        { "403.10" , "Forbidden: web server is configured to deny Execute access"},
        { "403.11" , "Forbidden: Password has been changed"},
        { "403.12" , "Mapper denied access"},
        { "403.13" , "Client certificate revoked"},
        { "403.14" , "Directory listing denied. The Web server is configured to not list the contents of this directory. This error typically comes if you do not have the default document configured and the request has come to the root of the site. The default document is the web page that is displayed at the root URL for a website. The first matching file in the list is used. Web apps might use modules that route based on URL, rather than serving static content, in which case there is no default document as such. For more details refer to <a href='https://docs.microsoft.com/en-us/azure/app-service/web-sites-configure#default-documents' target='_blank'>Configuring Default Documents in Azure App Service</a>"},
        { "403.15" , "Forbidden: Client access licenses have exceeded limits on the web server"},
        { "403.16" , "Client certificate is untrusted or invalid"},
        { "403.17" , "Client certificate has expired or is not yet valid"},
        { "403.18" , "Cannot execute requested URL in the current application pool"},
        { "403.19" , "Cannot execute CGI applications for the client in this application pool"},
        { "403.20" , "Forbidden: Passport logon failed"},
        { "403.21" , "Forbidden: Source access denied"},
        { "403.22" , "Forbidden: Infinite depth is denied"},
        { "403.23" , "Lock Token required"},
        { "403.24" , "Validation Failure"},
        { "403.501" , "[Dynamic IP Restriction] The request was rejected due to 'Deny by concurrent requests' rule of Dyanmic IP Address restrictions. Scroll below to the **Rejected Client IP** section to check the list of rejected client IP Addresses."},
        { "403.502" , "[Dynamic IP Restriction] The request was rejected due to 'Deny by request rate' rule of Dyanmic IP Address restrictions. Scroll below to the **Rejected Client IP** section to check the list of rejected client IP Addresses."},
        { "403.503" , "[IP Restriction] The request was rejected due to 'Deny by IP address' rule configured for Static IP address restrictions. Scroll below to the **Rejected Client IP** section to check the list of rejected client IP Addresses."},
        { "403.504" , "[IP Restriction] The request was rejected due to 'Deny by host name' rule configured for Static IP address restrictions. Scroll below to the **Rejected Client IP** section to check the list of rejected client IP Addresses."},
        { "403.60" , "Cross Site Request Forgery Unauthorized "},
        { "403.61" , "BasicAuth header missing" },
        { "403.62" , "BasicAuth address banned" },
        { "403.63" , "BasicAuth header invalid" },
        { "403.64" , "BasicAuth header Unauthenticated" },
        { "403.65" , "BasicAuth header Unauthorized" },
        { "403.66" , "CORS Unauthorized" },
        { "403.67" , message403_67},
        { "403.70" , "Client Cert generic failure"},
        { "403.71" , "Client Cert nego failure (call to NegotiateClientCert failed)"},
        { "403.72" , "Client Cert nego failure (call to NegotiateClientCert failed)"},
        { "403.73" , "HostNames Disabled"},
        { "403.74" , "The request failed due to **IP Address restrictions** configured on the App. Scroll below to the ***Rejected Client IP*** section to check the list of rejected client IP Addresses."},
        { "403.75" , "HttpScale Forbidden (Invalid forward token)"},
        { "403.76" , "The request was rejected due to a user-defined authorization rule (e.g. Azure AD security group restrictions or URL authorization rules)"},
        { "403.81" , "BearerAuthHeaderUnauthenticated"},
        { "403.82" , "BearerAuthHeaderUnauthorized"},
        { "403.91" , "BadRequestTriggersTooLarge"},
        { "403.92" , "RuntimeTokenInvalid"},
        { "404.1" , "Site not found"},
        { "404.2" , "ISAPI or CGI Restriction"},
        { "404.3" , "(Denied by mimemap)-MIME type restriction. Please follow the steps mentioned in <a href='https://blogs.msdn.microsoft.com/azureossds/2016/06/15/media-files-http-404-azure-web-apps/' target='_blank'>this</a> article to configure the required mime types."},
        { "404.4" , "No Handler Configured"},
        { "404.5" , "Denied by request filtering configuration"},
        { "404.6" , "Verb Denied"},
        { "404.7" , "File extension denied"},
        { "404.8" , "Hidden segment"},
        { "404.9" , "File attribute hidden"},
        { "404.10" , "Request header too long"},
        { "404.11" , "Request contains double escape sequence"},
        { "404.12" , "Request contains high-bit characters"},
        { "404.13" , "Content length too large"},
        { "404.14" , "Request URL too long"},
        { "404.15" , "Query string too long"},
        { "404.16" , "DAV request sent to the static file handler"},
        { "404.17" , "Preconditioned handler (Dynamic content mapped to the static file handler via a wildcard MIME mapping)"},
        { "404.18" , "Query string sequence denied"},
        { "404.19" , "Denied by filtering rule"},
        { "404.20" , "Too many URL segments"},
        { "404.24", "Request Throttler or HostMapper: Empty Host"},
        { "404.501" , "[Dynamic IP Restriction] Deny by concurrent requests"},
        { "404.502" , "[Dynamic IP Restriction] Deny by request rate"},
        { "404.503" , "[IP Restriction] Deny by IP address"},
        { "404.504" , "[IP Restriction] Deny by host name"},

    };

    errorCodes.Add("401.0", "Access Denied");
    errorCodes.Add("404.0", "Not Found. The resource that you are requesting does not exist.");
    errorCodes.Add("405.0", "Method not allowed. This typically means that the request is made over a HTTP VERB that is not allowed by the application code (For e.g., your app may only be allowing HTTP GET or HTTP POST but the request came over HTTP PUT)");
    errorCodes.Add("406.0", "Client browser does not accept the MIME type of the requested page");
    errorCodes.Add("407.0", "Proxy Authorization required");
    errorCodes.Add("408.0", "Request timed out");
    errorCodes.Add("409.0","Conflict");
    errorCodes.Add("412.0", "Precondition failed");
    errorCodes.Add("413.0","Request entity too large");
    errorCodes.Add("414.0","URL too long");
    errorCodes.Add("415.0","Unsupported media type");
    errorCodes.Add("416.0","Requested range not satisfiable");
    errorCodes.Add("417.0","Expectation failed");
    errorCodes.Add("422.0","Unprocessable entity");
    errorCodes.Add("424.0","Failed Dependency");
    errorCodes.Add("429.0","Too Many Requests - Retry");
    errorCodes.Add("499.0","Client Closed Request - nginx");

    if (isInternal)
    {
        var errorCodesInternal = new Dictionary<string, string>()
        {
            { "404.60" , "Live Upgrade in progress"},
            { "404.63" , "Site not found: Hostname lookup failed, we are probably under attack"},
            { "404.68" , "Empty Host Header: We are being pinged/attacked by someone, or requests are coming from HTTP1.0 clients (unlikely)"},
            { "404.71", "Worker not found"},
            { "404.72", "No Capacity in stamp"},
            { "404.90", "Blocked Host Name"},
            { "423.1" , "Lock token submitted"},
            { "423.2" , "No Conflicting lock"}
        };

        //https://stackoverflow.com/questions/294138/merging-dictionaries-in-c-sharp
        errorCodesInternal.ToList().ForEach(x => errorCodes.Add(x.Key, x.Value));
    }
    return errorCodes;
}

static string GetQuotaViolationMessage(OperationContext<App> cxt, string sku)
{
    if (sku.ToLower() == "free" || sku.ToLower() == "shared")
    {
        return $". Check the { DetectorUtils.GetDetectorLink(cxt, "detectors/AntaresQuota", "Quota Violation Events", true) } detector to understand which Quota has been hit.";
    }
    return "";
}

static Dictionary<string, string> GetHandlerDescriptions(bool isInternal)
{
    string extensionLessDescription = "Extensionless URL Handler is used by IIS to pass requests for extension less URLs to your ASP.NET MVC web app. ";
    string staticFileHandler = "Static file handler is the handler used by IIS to serve static files for your app. Check the description next to the substatus code to understand why you got the HTTP 4xx error";
    var handlerDescription = new Dictionary<string, string>();

    handlerDescription.Add("StaticFile", staticFileHandler);
    handlerDescription.Add("aspNetCore", "aspNetCore handler processes all requests to your ASP.NET Core app.");
    handlerDescription.Add("System.Web.Http.WebHost.HttpControllerHandler", "HttpControllerHandler is the handler used by ASP.NET to server Web API requests. Errors set by this handler would typically mean application code setting a response status by calling CreateResponse method passing one of the <a href='https://docs.microsoft.com/en-us/dotnet/api/system.net.httpstatuscode?view=netframework-4.7.2' target='_blank'>HttpStatusCode</a>  enumeration.");
    handlerDescription.Add("ExtensionlessUrlHandler", extensionLessDescription);
    handlerDescription.Add("System.Web.Mvc.MvcHandler", "MvcHandler is used by ASP.NET MVC to select the appropriate controller based on the request");
    handlerDescription.Add("iisnode", "IISNODE is a native IIS module that allows hosting of node.js applications in IIS. ");
    handlerDescription.Add("WebServiceHandlerFactory", "WebServiceHandlerFactory is used by ASP.NET to process incoming HTTP Web Service requests (typically .asmx files)");
    handlerDescription.Add("httpPlatformHandler", "HttpPlatformHandler acts a gateway to a lot of different application types like ASP.NET Core, NodeJs, JAVA etc. ");
    handlerDescription.Add("PageHandlerFactory", "The PageHandlerFactory class is the default handler factory implementation for ASP.NET pages.");
    handlerDescription.Add("svc-Integrated", "Svc Handler serves all the WCF service requests for your app");
    handlerDescription.Add("ExtensionlessUrl", extensionLessDescription);
    handlerDescription.Add("StaticFileHandler", staticFileHandler);
    handlerDescription.Add("ScriptResource", "ScriptResourceHandler processes all requests for embedded script files that are referenced through the ScriptManager class.");
    handlerDescription.Add("UrlRoutingModule-4.0", "UrlRoutingModule helps in routing requests to an ASP.NET MVC-based Web application");
    handlerDescription.Add("via_FastCGI", "The Fast CGI handler is used to support various CGI and Open source stacks like PHP, Java etc.");
    return handlerDescription;
}

static string GetHandlerDescription(bool isInternal, string handlerName)
{
    string descriptionText = "";

    var handlerDescriptions = GetHandlerDescriptions(isInternal);
    foreach (string key in handlerDescriptions.Keys)
    {
        if (handlerName.ToLower().Contains(key.ToLower()))
        {
            descriptionText = handlerDescriptions[key];
            break;
        }
    }

    return descriptionText;
}

static string GetHandlerDescriptionMarkdown(bool isInternal, DataTable http4xxByModule)
{
    var markDownBuilder = new StringBuilder();
    var descriptions = new Dictionary<string, string>();
    foreach (string handler in http4xxByModule.Rows.Cast<DataRow>().GroupBy(row => row["Handler"]).Select(x => x.Key))
    {
        if (!descriptions.ContainsKey(handler))
        {
            descriptions.Add(handler, GetHandlerDescription(isInternal, handler));
        }
    }

    if (descriptions.Count > 0)
    {
        markDownBuilder.AppendLine("");
        markDownBuilder.AppendLine("Handler descriptions:");
        markDownBuilder.AppendLine("");
        foreach (var item in descriptions)
        {
            markDownBuilder.AppendLine($"+ **{item.Key}** - { item.Value }");
        }
    }

    return markDownBuilder.ToString();
}

private static string GetErrorDetailsFromFreb(OperationContext<App> cxt)
{
    return
    $@"AntaresWebWorkerFREBLogs
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
        | where SiteName =~ '{cxt.Resource.Name}' or SiteName startswith '{cxt.Resource.Name}__'
        | where StatusCode >=400 and StatusCode < 500
        | where Details has 'MODULE_SET_RESPONSE_STATUS_ERROR'
        | extend Details = split(Details, '|#')
        | mvexpand Details
        | where Details has 'MODULE_SET_RESPONSE_STATUS_ERROR'
        | parse Details with* 'Module: ' FailingModule ',' *
        | summarize FailedRequestCount=count() by TIMESTAMP=bin(PreciseTimeStamp, 5m), SiteName, FailingModule = strcat(FailingModule, '(', StatusCode, '.', HttpSubStatus, ')')";
}

private static string GetHttp4xxTimelineWorker(OperationContext<App> cxt)
{
    return
    $@"AntaresIISLogWorkerTable
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
        | where Sc_status >=400 and Sc_status < 500
        | where User_agent != 'AlwaysOn'
        | where S_sitename =~ '{cxt.Resource.Name}' or S_sitename startswith '{cxt.Resource.Name}__'
        | summarize Errors=count() by TIMESTAMP=bin(TIMESTAMP,5m), S_sitename, HTTP=strcat(Sc_status,'.', Sc_substatus)
        | order by TIMESTAMP asc";
}

private static string GetHttp4xxDetailsWorker(OperationContext<App> cxt)
{
    return
    $@"AntaresIISLogWorkerTable
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
        | where Sc_status >=400 and Sc_status < 500
        | where User_agent != 'AlwaysOn'
        | where S_sitename =~ '{cxt.Resource.Name}' or S_sitename startswith '{cxt.Resource.Name}__'
        | summarize Errors=count() by TIMESTAMP=bin(TIMESTAMP,5m), S_sitename ,Sc_status, Sc_substatus, Win32Status=Sc_win32_status,S_reason
        | order by Errors desc";
}
private static string GetHttp4xxTimelineFrontEnd(OperationContext<App> cxt, IEnumerable<string> hostnames)
{
    var hostnamesQuery = Utilities.HostNamesFilterQuery(hostnames);
    hostnamesQuery = hostnamesQuery.Replace("\"orCs_host", "\" or Cs_host");
    //"orCs_host
    return
    $@"AntaresIISLogFrontEndTable
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
        | where Sc_status >=400 and Sc_status < 500
        | where User_agent != 'AlwaysOn'
        | where {hostnamesQuery}
        | summarize Errors=count() by TIMESTAMP=bin(TIMESTAMP,5m), HTTP=strcat(Sc_status,'.', Sc_substatus)
        | order by TIMESTAMP asc";
}

private static string GetHttp4xxTimelineFrontEndByModuleLoggedByDwas(OperationContext<App> cxt, IEnumerable<string> hostnames)
{
    var hostnamesQuery = Utilities.HostNamesFilterQuery(hostnames);
    hostnamesQuery = hostnamesQuery.Replace("\"orCs_host", "\" or Cs_host");
    //"orCs_host
    return
    $@"AntaresIISLogFrontEndTable
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
        | where Sc_status >=400 and Sc_status < 500
        | where User_agent != 'AlwaysOn'
        | where {hostnamesQuery}
        | where Cs_uri_query contains 'DWAS-Handler-Name='
        | parse Cs_uri_query with * ""DWAS-Handler-Name="" * ""|"" * ""|"" * ""|""* ""|"" * ""|"" HandlerName ""|"" *
        | summarize Errors=count() by TIMESTAMP=bin(TIMESTAMP,5m), Handler=HandlerName , StatusCode = Sc_status, HandlerName = strcat(HandlerName,'-',Sc_status)
        | order by TIMESTAMP asc";
}

private static string GetHttp4xxTimelineFrontEndByHostname(OperationContext<App> cxt, IEnumerable<string> hostnames)
{
    var hostnamesQuery = Utilities.HostNamesFilterQuery(hostnames);
    hostnamesQuery = hostnamesQuery.Replace("\"orCs_host", "\" or Cs_host");

    return
    $@"AntaresIISLogFrontEndTable
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
        | where Sc_status >=400 and Sc_status < 500
        | where User_agent != 'AlwaysOn'
        | where {hostnamesQuery}
        | summarize Errors=count() by TIMESTAMP=bin(TIMESTAMP,5m), Host=Cs_host
        | order by TIMESTAMP asc";
}

private static string GetHttp4xxDetailsFrontEnd(OperationContext<App> cxt, IEnumerable<string> hostnames)
{
    var hostnamesQuery = Utilities.HostNamesFilterQuery(hostnames);
    hostnamesQuery = hostnamesQuery.Replace("\"orCs_host", "\" or Cs_host");
    //"orCs_host

    return
    $@"AntaresIISLogFrontEndTable
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
        | where Sc_status >=400 and Sc_status < 500
        | where User_agent != 'AlwaysOn'
        | where {hostnamesQuery}
        | summarize Errors=count() by Sc_status, Sc_substatus
        | order by Errors desc";
}

private static string GetFailedUrlsByStatusCodeFrontEnd(OperationContext<App> cxt, IEnumerable<string> hostnames, int failedRequestCount)
{
    var hostnamesQuery = Utilities.HostNamesFilterQuery(hostnames);
    hostnamesQuery = hostnamesQuery.Replace("\"orCs_host", "\" or Cs_host");
    //"orCs_host

    return
    $@"AntaresIISLogFrontEndTable
        | where {Utilities.TimeAndTenantFilterQuery(cxt.StartTime, cxt.EndTime, cxt.Resource)}
        | where Sc_status >=400 and Sc_status < 500
        | where User_agent != 'AlwaysOn'
        | where {hostnamesQuery}
        | top-nested  10 of Sc_status  by count(),top-nested 10 of Sc_substatus by count(),top-nested 10 of Cs_method by count(), top-nested {failedRequestCount} of Cs_uri_stem by count()
        | project-away aggregated_Sc_status 
        | extend RequestCount = aggregated_Cs_uri_stem 
        | extend HttpStatus = Sc_status 
        | extend HttpSubStatus = Sc_substatus 
        | extend Url = Cs_uri_stem 
        | extend Method = Cs_method 
        | project-away aggregated_Cs_uri_stem , aggregated_Sc_substatus , Sc_status , Sc_substatus , Cs_uri_stem, Cs_method 
        | order by RequestCount desc
        | project HttpStatus , HttpSubStatus, Method, RequestCount , Url";
}

[AppFilter(AppType = AppType.All, PlatformType = PlatformType.Windows | PlatformType.HyperV, StackType = StackType.All, InternalOnly = false)]
[Definition(Id = "http4xx0", AnalysisType="appDownAnalysis", Category = Categories.AvailabilityAndPerformance, Name = "HTTP 4xx Errors", Author = "puneetg", Description = "This view helps you identify all the HTTP 4XX requests for your app and provides insights on common solutions that you can take to further investigate and resolve these errors.")]
public async static Task<Response> Run(DataProviders dp, OperationContext<App> cxt, Response res)
{
    string stampName = cxt.Resource.Stamp.Name;
    string[] ColourValues = new string[] {
        "#FF0000", "#800000", "#808000", "#800080", "#008080", "#808080",
        "#C00000", "#00C000", "#0000C0", "#C0C000", "#C000C0", "#00C0C0", "#C0C0C0",
        "#400000", "#004000", "#000040", "#404000", "#400040", "#004040", "#404040",
        "#200000", "#002000", "#000020", "#202000", "#200020", "#002020", "#202020",
        "#600000", "#006000", "#000060", "#606000", "#600060", "#006060", "#606060",
        "#A00000", "#00A000", "#0000A0", "#A0A000", "#A000A0", "#00A0A0", "#A0A0A0",
        "#E00000", "#00E000", "#0000E0", "#E0E000", "#E000E0", "#00E0E0", "#E0E0E0",
    };

    const int MAX_REQUEST_URLS_TO_DISPLAY = 15;

    var http4xxSummary = new Dictionary<int, int>();
    var frebErrorsTask = dp.Kusto.ExecuteQuery(GetErrorDetailsFromFreb(cxt), cxt.Resource.Stamp.Name, null, "GetErrorDetailsFromFreb");
    var slotTimeRangesTask = dp.Observer.GetRuntimeSiteSlotMap(cxt.Resource.Stamp.InternalName, cxt.Resource.Name);
    var http4xxTimelineTaskWorker = dp.Kusto.ExecuteQuery(GetHttp4xxTimelineWorker(cxt), cxt.Resource.Stamp.Name, null, "GetHttp4xxTimelineWorker");
    var http4xxTableTaskWorker = dp.Kusto.ExecuteQuery(GetHttp4xxDetailsWorker(cxt), cxt.Resource.Stamp.Name, null, "GetHttp4xxDetailsWorker");
    var http4xxTimelineTaskFrontEnd = dp.Kusto.ExecuteQuery(GetHttp4xxTimelineFrontEnd(cxt, cxt.Resource.Hostnames), cxt.Resource.Stamp.Name, null, "GetHttp4xxTimelineFrontEnd");
    var http4xxTableTaskFrontEnd = dp.Kusto.ExecuteQuery(GetHttp4xxDetailsFrontEnd(cxt, cxt.Resource.Hostnames), cxt.Resource.Stamp.Name, null, "GetHttp4xxDetailsFrontEnd");
    var http4xxTimelineFrontEndHostnameTask = dp.Kusto.ExecuteQuery(GetHttp4xxTimelineFrontEndByHostname(cxt, cxt.Resource.Hostnames), cxt.Resource.Stamp.Name, null, "GetHttp4xxTimelineFrontEndByHostname");
    var http4xxTimelineFrontEndFrontEndByModuleLoggedByDwasTask = dp.Kusto.ExecuteQuery(GetHttp4xxTimelineFrontEndByModuleLoggedByDwas(cxt, cxt.Resource.Hostnames), cxt.Resource.Stamp.Name, null, "GetHttp4xxTimelineFrontEndByModuleLoggedByDwas");
    var http4xxfailedUrlsTask = dp.Kusto.ExecuteQuery(GetFailedUrlsByStatusCodeFrontEnd(cxt, cxt.Resource.Hostnames, MAX_REQUEST_URLS_TO_DISPLAY), cxt.Resource.Stamp.Name, null, "GetFailedUrlsByStatusCodeFrontEnd");
    
    var easyAuthFailures = false;

    var http4xxTableWorker = await http4xxTableTaskWorker;
    var http4xxTableFrontEnd = await http4xxTableTaskFrontEnd;
    var http4xxTimelineFrontEnd = await http4xxTimelineTaskFrontEnd;
    var http4xxTimelineFrontEndHostname = await http4xxTimelineFrontEndHostnameTask;
    var http4xxTimelineFrontEndFrontEndByModuleLoggedByDwas = await http4xxTimelineFrontEndFrontEndByModuleLoggedByDwasTask;
    var http4xxfaildUrls = await http4xxfailedUrlsTask;
    var errorsCountPerInstanceTask = dp.Kusto.ExecuteQuery(InstanceView.GetErrorsPerInstance(cxt, "4xx"), cxt.Resource.Stamp.Name, null, "GetRunningInstanceDetails");
    var http4xxCountPerInstancePer5MinutesTask = dp.Kusto.ExecuteQuery(InstanceView.GetHttpErrorsCountPerInstancePer5Minutes(cxt, "4xx"), stampName, null, "GetHttp4xxCountPerInstancePer5Minutes");
    var skuTask = Availability.GetMdmIdAndSkuForServerFarm(cxt, dp);

    var slotTimeRanges = await slotTimeRangesTask;
    http4xxTableWorker = Utilities.GetSlotEvents(cxt.Resource.Slot, slotTimeRanges, http4xxTableWorker, "S_sitename", "TIMESTAMP");
    RemoveColumnsSafely(http4xxTableWorker, new string[] {"S_sitename", "TIMESTAMP"});

    List<HttpStatusDetails> http4xxTableCombinedList = http4xxTableWorker.Rows.Cast<DataRow>()
          .GroupBy(x => new { Sc_status = x["Sc_status"].ToString(), Sc_substatus = x["Sc_substatus"].ToString(), /*Win32Status = x["Win32Status"].ToString(),*/ S_Reason = x["S_Reason"].ToString() })
          .Select(x => new HttpStatusDetails
          {
              HttpStatus = x.Key.Sc_status,
              HttpSubStatus = x.Key.Sc_substatus,
              S_Reason = x.Key.S_Reason,
              Instance = "Worker",
              Errors = x.Sum(z => Convert.ToInt32(z["Errors"]))
          }).ToList();

    foreach (DataRow dr in http4xxTableFrontEnd.Rows)
    {
        if (!http4xxTableCombinedList.Any(x => x.HttpStatus == dr["Sc_status"].ToString() && x.HttpSubStatus == dr["Sc_substatus"].ToString()))
        {
            if (!http4xxTableCombinedList.Any(worker => worker.HttpStatus == dr["Sc_status"].ToString() && !string.IsNullOrWhiteSpace(worker.HttpSubStatus) && worker.HttpSubStatus != "0" && dr["Sc_substatus"].ToString() == "0"))
            {
                HttpStatusDetails statusDetailsFrontEndOnly = new HttpStatusDetails
                {
                    HttpStatus = dr["Sc_status"].ToString(),
                    HttpSubStatus = dr["Sc_substatus"].ToString(),
                    Instance = "FrontEnd",
                    Errors = Convert.ToInt32(dr["Errors"])
                };
                http4xxTableCombinedList.Add(statusDetailsFrontEndOnly);
            }
        }
    }
    var http4xxTableCombined = http4xxTableCombinedList.OrderByDescending(x => x.Errors).PropertiesToDataTable<HttpStatusDetails>();

    RemoveColumnsSafely(http4xxTableCombined, new string[] {"Win32Status"});
    http4xxTableCombined.Columns.Add("Description");
    var skuAndMdmId = await skuTask;

    var errorCodes = InitializeErrors(cxt, skuAndMdmId.Item1);

    foreach (DataRow row in http4xxTableCombined.Rows)
    {
        string errorCode = row["HttpStatus"].ToString() + "." + row["HttpSubStatus"].ToString();
        if (errorCodes.ContainsKey(errorCode))
        {
            row["Description"] = errorCodes[errorCode].ToString();
        }
        if (!string.IsNullOrWhiteSpace(row["S_Reason"].ToString()))
        {
            row["Description"] = row["Description"].ToString() + " HTTP.SYS Error = " + row["S_Reason"].ToString();
        }

        if (int.TryParse(row["HttpStatus"].ToString(), out int serverErrorCode))
        {
            if (int.TryParse(row["Errors"].ToString(), out int errors))
            {
                if (!http4xxSummary.ContainsKey(serverErrorCode))
                {
                    http4xxSummary.Add(serverErrorCode, errors);
                }
                else
                {
                    http4xxSummary[serverErrorCode] = http4xxSummary[serverErrorCode] + errors;
                }

                if (int.TryParse(row["HttpSubStatus"].ToString(), out int errorSubStatus))
                {
                    if (row["Instance"].ToString() == "Worker" && (serverErrorCode >= 401 && serverErrorCode <= 403) && (errorSubStatus >= 60 && errorSubStatus <= 83))
                    {
                        easyAuthFailures = true;
                        row["Description"] = "EasyAuth:" + Enum.GetName(typeof(EasyAuthSubStatus), errorSubStatus);
                        row["Description"] += ". For more details, refer to <a href='https://github.com/cgillum/easyauth/wiki/HTTP-Status-Codes' target='_blank'>HTTP Status Codes by EasyAuth Module</a>";
                    }
                }
            }

        }
    }

    if (easyAuthFailures)
    {
        // We might need a new detector here. The existing detector isn't that good
        // res.AddDetectorCollection(new List<string>() { "AuthenticationandAuthorizationV2" });
    }
    var dynamicIpIssueExists = http4xxTableCombined.Rows.Cast<DataRow>().Any(x => (x["HttpStatus"].ToString() == "401" || x["HttpStatus"].ToString() == "403" || x["HttpStatus"].ToString() == "404") && (x["HttpSubStatus"].ToString() == "74" || x["HttpSubStatus"].ToString() == "501" || x["HttpSubStatus"].ToString() == "502" || x["HttpSubStatus"].ToString() == "503" || x["HttpSubStatus"].ToString() == "504"));


    var summary = new List<DataSummary>();
    int i = 0;
    foreach (var key in http4xxSummary.Keys)
    {
        var color = "blue";
        if (i < ColourValues.Length)
        {
            color = ColourValues[i];
        }
        DataSummary s = new DataSummary("HTTP - " + key.ToString(), http4xxSummary[key].ToString(), color);
        summary.Add(s);
        ++i;
    }

    RemoveColumnsSafely(http4xxTableCombined, new string[] {"S_Reason"});

    var errorTableMarkdown = DataTableToMarkdown(http4xxTableCombined);

    if (http4xxTableWorker.Rows.Count > 0 || http4xxTableFrontEnd.Rows.Count > 0)
    {
        var http4xxByModule = http4xxTimelineFrontEndFrontEndByModuleLoggedByDwas.Rows.Cast<DataRow>()
          .GroupBy(x => new { HandlerName = x["Handler"].ToString(), StatusCode = x["StatusCode"].ToString() })
          .Select(x => new FrebModule
          {
              HandlerName = x.Key.HandlerName,
              StatusCode = x.Key.StatusCode,
              FailedRequestCount = x.Sum(z => Convert.ToInt32(z["Errors"]))
          })
          .OrderByDescending(r => r.FailedRequestCount)
          .PropertiesToDataTable<FrebModule>();

        string descriptionText = "The below table shows you the count of all HTTP 4xx errors that happened for your app. The errors are categorized as Front End or Worker based on the instance that returned the error.";

        string desc2 = @"
        **Front End** in Azure App Service is a layer seven-load balancer, acting as a proxy, distributing incoming HTTP requests between different applications and their respective Workers.
        **Web Workers** are the backbone of the App Service scale unit and they run your application code.
        " + Environment.NewLine + Environment.NewLine;

        string markDown = $"<markdown>{descriptionText} {desc2} {errorTableMarkdown} {Environment.NewLine} </markdown>";
        var insightDetails = new Dictionary<string, string>();
        insightDetails.Add("Description", markDown);
        insightDetails.Add("More Information", "To know more about Front Ends, Workers and to understand internals of Azure App service Architecture, please read the article <a href='https://msdn.microsoft.com/en-us/magazine/mt793270.aspx' target='_blank'>Azure - Inside the Azure App Service Architecture</a>.");

        var insight = new Insight(InsightStatus.Warning, "HTTP 4XX requests detected", insightDetails, true);
        res.AddDataSummary(summary);
        res.AddInsight(insight);

        var whatCausesInsightDetails = new Dictionary<string, string>();
        whatCausesInsightDetails.Add("Description",@"<markdown>
        
        HTTP 4XX errors are referred as <strong>Client-side</strong> errors and these are the common reasons why they occur:-

        1. The HTTP request packet is malformed or not adhering to HTTP RFC specifications. 
        2. The app is enforcing authentication but the request is anonymous or invalid credntials are specified.
        3. A page or resource is requested that does not exist.
        4. The URL is long or contains restricted charachters.
        5. The Client-IP accessing the app is denied due to IP Address restrictions.
        6. The app is in a stopped state or has hit some quota enforced by Azure App Service. 
        
        Based on the configuration of your app, these errors ***may be expected*** and may not always indicate something wrong
        
        </markdown>");
        res.AddInsight(InsightStatus.Info,"What causes a HTTP 4XX error?", whatCausesInsightDetails);

        //
        // Now show all the Status Codes  as dropdown allowing the end user to click and choose
        //

        if (http4xxfaildUrls.Rows.Count > 0)
        {
            List<Tuple<string, bool, Response>> dropdownData = new List<Tuple<string, bool, Response>>();
            bool selected = true;
            foreach (var statusCode in http4xxfaildUrls.Rows.Cast<DataRow>().Select(row => row["HttpStatus"].ToString()).Distinct())
            {
                Response perStatusCodeResponse = new Response();
                DataTable perStatusDataTable = http4xxfaildUrls.Clone();
                foreach (DataRow dr in http4xxfaildUrls.Rows.Cast<DataRow>().Where(row => row["HttpStatus"].ToString() == statusCode).OrderByDescending(row => row["RequestCount"]))
                {
                    perStatusDataTable.ImportRow(dr);
                }

                perStatusCodeResponse.Dataset.Add(new DiagnosticData()
                {
                    Table = perStatusDataTable,
                    RenderingProperties = new TableRendering()
                    {
                        Title = $"Failed Requests By URI for HTTP Status Code - {statusCode}"
                    }
                });

                //perStatusCodeResponse.AddMarkdownView(DataTableToMarkdown(perStatusDataTable), "Failed Requests By Uri");

                dropdownData.Add(new Tuple<string, bool, Response>(
                $"HTTP Status {statusCode}",
                selected,
                perStatusCodeResponse));
                selected = false;
            }

            Dropdown dropdown = new Dropdown($"Choose a HTTP Status Code to view Top {MAX_REQUEST_URLS_TO_DISPLAY} Request Urls that failed with that status code : ", dropdownData);
            res.AddDropdownView(dropdown, "Failed Request URLs grouped by HTTP Status Code");
        }

        var http4xxTimelineWorker = await http4xxTimelineTaskWorker;
        http4xxTimelineWorker = Utilities.GetSlotEvents(cxt.Resource.Slot, slotTimeRanges, http4xxTimelineWorker, "S_sitename", "TIMESTAMP");
        RemoveColumnsSafely(http4xxTimelineWorker, new string[] {"S_sitename"});

        res.Dataset.Add(new DiagnosticData()
        {
            Table = http4xxTimelineFrontEnd,
            RenderingProperties = new TimeSeriesRendering()
            {
                Title = "Http 4xx Errors - All Front Ends",
                GraphType = TimeSeriesType.LineGraph,
                Description = "The below graph shows all the HTTP 4XX errors that were logged on the Front End for your app. Front End in Azure App Service is a layer seven-load balancer, acting as a proxy, distributing incoming HTTP requests between different applications and their respective Workers. The error count is categorized by the HTTP Status and Substatus code. Errors shown below are a super-set of errors logged on the Workers."
            }

        });

        int distinctHostNames = http4xxTimelineFrontEndHostname.Rows.Cast<DataRow>()
                .Select(r => r["Host"])
                .Distinct()
                .Count();

        if (http4xxTimelineWorker.Rows.Count > 0)
        {
            res.Dataset.Add(new DiagnosticData()
            {
                Table = http4xxTimelineWorker,
                RenderingProperties = new TimeSeriesRendering()
                {
                    Title = "Http 4xx Errors - All Workers",
                    GraphType = TimeSeriesType.LineGraph,
                    Description = "The below graph shows all the HTTP 4XX errors that were logged on the Worker serving your app. Web Workers are the backbone of the App Service scale unit and they run your application code. The error count is categorized by the HTTP Status and Substatus code"
                }
            });
        }
                
        //Insight for errors count per instance
        var inStatus = InsightStatus.Success;
        var insightOverview = new Dictionary<string,string>();
        var httpErrorsTaskDetails = await errorsCountPerInstanceTask;

        if (cxt.IsInternalCall)
        {

        
            // By default set the status to be success
            int status = 0; 
            string vmsMarkdown = InstanceView.DataTableToMarkdown(httpErrorsTaskDetails, stampName, cxt, ref status);
            string vmsDescription = $"The below table shows the 4xx errors on all VM instances which were running the web app in the time frame. **Ctrl+Click an instance below** directs you to *Jarvis Worker Dashboard* where you will get a quick overview of basic worker signals." + Environment.NewLine + Environment.NewLine;
            string vmsInsight = $"<markdown>{vmsDescription}{vmsMarkdown}</markdown>";
            insightOverview.Add("Errors count", vmsInsight);

            var insightHttp4xxTitle = "No Http 4xx Detected on all the Workers";
            if(status > 0)
            {
                inStatus = InsightStatus.Critical;
                insightHttp4xxTitle = "Http 4xx Errors Detected on Workers";
            }

            Insight insightHttp4xx = new Insight(inStatus, insightHttp4xxTitle, insightOverview);
            insightHttp4xx.IsExpanded = true;
            res.AddInsights(new List<Insight>() { insightHttp4xx });

            if (http4xxTimelineWorker.Rows.Count > 0)
            {
                        //Chart of Http 4xx per instance
                res.Dataset.Add(new DiagnosticData()
                {
                    Table = await http4xxCountPerInstancePer5MinutesTask,
                    RenderingProperties = new TimeSeriesRendering()
                    {
                        Title = "Http 4xx per Worker",
                        GraphType = TimeSeriesType.LineGraph
                    }

                });   
            }
        }


        if (http4xxByModule.Rows.Count > 0)
        {
            var handlerMarkdown = DataTableToMarkdown(http4xxByModule);
            string handlerDescriptionText = "The below table shows you the count of HTTP 4XX errors grouped by the HTTP Handler in the IIS Pipeline responsible for setting the HTTP Status code. This information is logged by the actual instance (Worker) serving your app. The handler name may help you identify where the error code might be originating from." + Environment.NewLine + Environment.NewLine;
            string handlerDescription = GetHandlerDescriptionMarkdown(cxt.IsInternalCall, http4xxTimelineFrontEndFrontEndByModuleLoggedByDwas);
            string markDownHandler = $"<markdown>{handlerDescriptionText}{handlerMarkdown}{handlerDescription}</markdown>";
            var insightDetailsHandler = new Dictionary<string, string>();
            insightDetailsHandler.Add("Description", markDownHandler);

            res.AddInsight(new Insight(InsightStatus.Warning, "HTTP 4XX Errors grouped by Handler setting the HTTP Status code", insightDetailsHandler, true));
        }

        var descriptionModule = "The below graph shows all the HTTP 4XX errors that were logged on the Worker serving your app grouped by the IIS or the .Net handler in the IIS pipeline that was responsibe for setting the last Status Code.";
        if (cxt.IsInternalCall)
        {
            descriptionModule += " This information is parsed by looking at the DWAS-Handler-Name field in the Cs_uri_query column of AntaresIISLogFrontEndTable";
        }

        RemoveColumnsSafely(http4xxTimelineFrontEndFrontEndByModuleLoggedByDwas, new string[] {"Handler", "StatusCode"});
        if (http4xxTimelineFrontEndFrontEndByModuleLoggedByDwas.Rows.Count > 0)
        {
            res.Dataset.Add(new DiagnosticData()
            {
                Table = http4xxTimelineFrontEndFrontEndByModuleLoggedByDwas,
                RenderingProperties = new TimeSeriesRendering()
                {
                    Title = "Http 4xx Errors - Grouped by the Handler Name",
                    GraphType = TimeSeriesType.BarGraph,
                    Description = descriptionModule
                }
            });
        }

        //
        // Plot the graph for HostNames only if the host names that we found
        // are more than 1 otherwise we are plotting 1 line with the single hos
        //
        if (distinctHostNames > 1)
        {
            var hostnamesGraph = new DiagnosticData()
            {
                Table = http4xxTimelineFrontEndHostname,
                RenderingProperties = new TimeSeriesRendering()
                {
                    Title = "Http 4xx Errors - All Front Ends By Hostname",
                    GraphType = TimeSeriesType.LineGraph,
                    Description = "The below graph shows all the HTTP 4XX errors categorized by the HostName used in the request"
                }
            };

            res.AddDynamicInsight(new DynamicInsight(
            InsightStatus.Info, //Status
            "HTTP 4XX errors categorized by the HostName used in the request", // Message to be displayed on insight
            hostnamesGraph, false));
        }

        if (dynamicIpIssueExists)
        {
            res.AddDetectorCollection(new List<string>() { "ipaddressrestrictions" });
        }

        //
        // Commenting this out as I feel this is redundant, will enable it if we have to
        // 

        // if (cxt.IsInternalCall)
        // {
        //     var frebErrorsTimeline = await frebErrorsTask;
        //     frebErrorsTimeline = Utilities.GetSlotEvents(cxt.Resource.Slot, slotTimeRanges, frebErrorsTimeline, "SiteName", "TIMESTAMP");
        //     frebErrorsTimeline.Columns.Remove("SiteName");

        //     if (frebErrorsTimeline.Rows.Count > 0)
        //     {
        //         res.Dataset.Add(new DiagnosticData()
        //         {
        //             Table = frebErrorsTimeline,
        //             RenderingProperties = new TimeSeriesRendering()
        //             {
        //                 Title = "Http 4XX Errors by Module",
        //                 GraphType = TimeSeriesType.BarGraph,
        //                 Description = "The below graph shows all the HTTP 4XX errors that were logged on the worker (actual instance) serving the app. The error count is categorized by the module in the IIS pipeline responsible for setting the failing status code"
        //             }
        //         });
        //     }
        // }
        
    }
    else
    {
        res.AddInsight(InsightStatus.Success, "No HTTP 4XX requests");
    }

    return res;
}

static DataTable PropertiesToDataTable<T>(this IEnumerable<T> source)
{
    DataTable dt = new DataTable();
    var props = TypeDescriptor.GetProperties(typeof(T));
    foreach (PropertyDescriptor prop in props)
    {
        DataColumn dc = dt.Columns.Add(prop.Name, prop.PropertyType);
        dc.Caption = prop.DisplayName;
        dc.ReadOnly = prop.IsReadOnly;
    }
    foreach (T item in source)
    {
        DataRow dr = dt.NewRow();
        foreach (PropertyDescriptor prop in props)
        {
            dr[prop.Name] = prop.GetValue(item);
        }
        dt.Rows.Add(dr);
    }
    return dt;
}

static string DataTableToMarkdown(DataTable dt)
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

static void RemoveColumnsSafely(DataTable tbl, string[] columns)
{
    var columnsToRemove = new List<DataColumn>();
    for (int i = 0; i < tbl.Columns.Count; i++)
    {
        foreach(string column in columns)
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
    foreach(var column in columnsToRemove)
    {
        tbl.Columns.Remove(column);
    }
}

class HttpStatusDetails
{

    public string HttpStatus { get; set; }
    public string HttpSubStatus { get; set; }
    public string Win32Status { get; set; }

    public string Instance { get; set; }
    public string S_Reason { get; set; }
    public int Errors { get; set; }
}

class HttpStatus
{
    public int StatusCode { get; set; }
    public int Substatus { get; set; }
    public double win32Status { get; set; }
    public HttpStatus(int _statusCode)
    {
        StatusCode = _statusCode;
    }

    public HttpStatus(int _statusCode, int _substatus)
    {
        StatusCode = _statusCode;
        Substatus = _substatus;
    }

    public HttpStatus(int _statusCode, int _substatus, double _win32Status)
    {
        StatusCode = _statusCode;
        Substatus = _substatus;
        win32Status = _win32Status;
    }
}

enum EasyAuthSubStatus
{
    None = 0,
    TooManyClients = 7,
    CrossSiteRequestForgeryUnauthorized = 60,
    SessionTimeout = 70,
    Unauthenticated = 71,
    LoggedOut = 72,
    LoginProtocolError = 73,
    ConfigurationError = 74,
    FragmentDataRequired = 75,
    AuthorizationCheckFailed = 76,
    ProviderTokenAccepted = 77,
    InfiniteRedirectLoop = 78,
    InternalModuleFailure = 79,
    RefreshTokenUnavailable = 80,
    RefreshTokenInvalid = 81,
    RefreshTokenNotSupported = 82,
    InvalidToken = 83
}

class FrebModule
{
    public string HandlerName { get; set; }
     public string StatusCode { get; set; }
    public int FailedRequestCount { get; set; }
}

