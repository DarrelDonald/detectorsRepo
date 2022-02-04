using System.Collections;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Diagnostics.DataProviders;
using Diagnostics.ModelsAndUtils.Attributes;
using Diagnostics.ModelsAndUtils.Models.ResponseExtensions;
using Diagnostics.ModelsAndUtils.ScriptUtilities;
using System.Reflection;
using Diagnostics.ModelsAndUtils;
using Diagnostics.ModelsAndUtils.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

private static string GetDiagWebsiteHostnameQueryClause(string dataSource){
     switch (dataSource)
    {
        case "1":   // Applens calls
            return $@"where DiagWebsiteHostName in ('diag-runtimehost-prod-euap-001', 'diag-runtimehost-prod-cuseuap-001')";
        case "2":
            return $@"where DiagWebsiteHostName in ('diag-runtimehost-prod-am2-001', 'diag-runtimehost-prod-bay-001', 'diag-runtimehost-prod-blu-001', 'diag-runtimehost-prod-db3-001', 'diag-runtimehost-prod-dm1-001', 'diag-runtimehost-prod-sg1-001')";
        default:
            return $@"where DiagWebsiteHostName in ('diag-runtimehost-prod-am2-001', 'diag-runtimehost-prod-bay-001', 'diag-runtimehost-prod-blu-001', 'diag-runtimehost-prod-db3-001', 'diag-runtimehost-prod-dm1-001', 'diag-runtimehost-prod-sg1-001', 'diag-runtimehost-prod-euap-001', 'diag-runtimehost-prod-cuseuap-001')";

    }
}

private static string GetClusterQuery(string dataSource, string tableName){
        switch (dataSource){
            case "1":   // Applens calls
                return $@"union (cluster('wawscus').database('wawsprod').{tableName}), (cluster('wawseus').database('wawsprod').{tableName})";
            default:
                return $@"union (cluster('wawscus').database('wawsprod').{tableName}), (cluster('wawseus').database('wawsprod').{tableName}), (cluster('wawsweu').database('wawsprod').{tableName}), (cluster('wawsneu').database('wawsprod').{tableName}), (cluster('wawseas').database('wawsprod').{tableName}), (cluster('wawswus').database('wawsprod').{tableName})";
    }
}

private static string GetAvailability(string detectorId, string dataSource, string timeRange){
    
    return
    $@"{GetClusterQuery(dataSource, "DiagnosticRole")}
    | where TIMESTAMP > ago({timeRange})
    | where DiagEnvironment == 'Production'
    | {GetDiagWebsiteHostnameQueryClause(dataSource)}
    | where EventId == 2002 and Address != '/healthping'
    | where Address has_cs strcat('/detectors/', '{detectorId}')
    | summarize PerformanceP50 = percentile(LatencyInMilliseconds, 50), PerformanceP90 = percentile(LatencyInMilliseconds, 90), Availabilty = 1.0*countif(StatusCode == 200)/count(), TotalRequests = count(), Total2xx = countif(StatusCode >= 200 and StatusCode <= 299), Total4xx = countif(StatusCode >= 400 and StatusCode <= 499), Total5xx = countif(StatusCode >= 500 and StatusCode <= 599)";
}

private static string GetAvailabilityByTime(string detectorId, string dataSource, string timeRange){
    return
    $@"{GetClusterQuery(dataSource, "DiagnosticRole")}
    | where TIMESTAMP > ago({timeRange})
    | where DiagEnvironment =~ 'Production'
    | {GetDiagWebsiteHostnameQueryClause(dataSource)}
    | where EventId == 2002 and Address != '/healthping'
    | where Address contains strcat('/detectors/', '{detectorId}')
    | summarize Availabilty = 1.0*countif(StatusCode == 200)/count(), TotalRequests = count(), Total2xx = countif(StatusCode >= 200 and StatusCode <= 299), Total4xx = countif(StatusCode >= 400 and StatusCode <= 499), Total5xx = countif(StatusCode >= 500 and StatusCode <= 599) by bin(TIMESTAMP, 5m)";
}

private static string GetExceptions(string detectorId, string dataSource, string timeRange){
    return
    $@"{GetClusterQuery(dataSource, "DiagnosticRole")}
    | where TIMESTAMP > ago({timeRange})
    | where DiagEnvironment =~ 'Production'
    | {GetDiagWebsiteHostnameQueryClause(dataSource)}
    | where EventId == 2002 and Address != '/healthping'
    | where Address contains strcat('/detectors/', '{detectorId}')
    | project RequestId, Address, StatusCode
    | join(
        {GetClusterQuery(dataSource, "DiagnosticRole")}
        | where TIMESTAMP > ago({timeRange})
        | where DiagEnvironment =~ 'Production'
        | {GetDiagWebsiteHostnameQueryClause(dataSource)}
        | where EventId == 2001
        | where Address contains strcat('/detectors/', '{detectorId}')
        | where ExceptionDetails !contains 'DataSource timed out: Kusto'
        | project PreciseTimeStamp, RequestId, ExceptionType, ExceptionDetails)
    on RequestId
    | summarize count(), argmax(PreciseTimeStamp, *) by ExceptionDetails
    | sort by count_
    | take 10
    | project count_, ExceptionType = max_PreciseTimeStamp_ExceptionType, ExceptionDetails, Address = max_PreciseTimeStamp_Address
    | extend AppLensUrl = strcat('https://applens-preview.azurewebsites.net', Address)
    | project Count = count_, ExceptionType, ExceptionDetails, AppLensUrl";
}

[SystemFilter]
[Definition(Id = "__monitoring", Name = "Detector Monitoring Statistics", Author = "xipeng,shgup,darreldonald", Description = "")]
public async static Task<Response> Run(DataProviders dp, Dictionary<string, dynamic> cxt, Response res)
{
    try {
         string detectorId = cxt["detectorId"].ToString().Replace(" ", "%20");
    string timeRange = cxt["timeRange"].ToString() + "h";
    string dataSource = cxt["dataSource"].ToString();



    // string query = GetAvailability(cxt["detectorId"].ToString().Replace(" ", "%20"), cxt["dataSource"].ToString(), "168" + "h");
    // res.AddInsight(InsightStatus.Info, query);
    // query = GetAvailabilityByTime(cxt["detectorId"].ToString().Replace(" ", "%20"), cxt["dataSource"].ToString(), cxt["timeRange"].ToString() + "h");
    // res.AddInsight(InsightStatus.Info, query);
    // query = GetExceptions(cxt["detectorId"].ToString().Replace(" ", "%20"), cxt["dataSource"].ToString(), cxt["timeRange"].ToString() + "h");
    // res.AddInsight(InsightStatus.Info, query);

    //Task<DataTable> availabilityTableTask = dp.Kusto.ExecuteQueryOnAllAppAppServiceClusters(GetAvailability(cxt["detectorId"].ToString().Replace(" ", "%20"), cxt["dataSource"].ToString(), cxt["timeRange"].ToString() + "h"), "GetDetectorAvailability");
    // Task<DataTable> availabilityTableTask = dp.Kusto.ExecuteQueryOnAllAppAppServiceClusters(GetAvailability(cxt["detectorId"].ToString().Replace(" ", "%20"), cxt["dataSource"].ToString(), "168" + "h"), "GetDetectorAvailability");
    // Task<DataTable> graphDataTask = dp.Kusto.ExecuteQueryOnAllAppAppServiceClusters(GetAvailabilityByTime(cxt["detectorId"].ToString().Replace(" ", "%20"), cxt["dataSource"].ToString(), cxt["timeRange"].ToString() + "h"), "GetDetectorAvailabilityTimeline");
    // Task<DataTable> exceptionsTask = dp.Kusto.ExecuteQueryOnAllAppAppServiceClusters(GetExceptions(cxt["detectorId"].ToString().Replace(" ", "%20"), cxt["dataSource"].ToString(), cxt["timeRange"].ToString() + "h"), "GetDetectorExceptionTable");
    
    //Task<DataTable> availabilityTableTask = dp.Kusto.ExecuteClusterQuery(GetAvailability(detectorId, dataSource, timeRange), "GetDetectorAvailability");
    //Task<DataTable> graphDataTask = dp.Kusto.ExecuteClusterQuery(GetAvailabilityByTime(detectorId, dataSource, timeRange), "GetDetectorAvailabilityTimeline");
    Task<DataTable> exceptionsTask = dp.Kusto.ExecuteClusterQuery(GetExceptions(detectorId, dataSource, timeRange), "GetDetectorExceptionTable");
    

    // DataTable availabilityTable =  await availabilityTableTask;
    // res.AddMarkdownView("availabilityTable");
    //if (availabilityTable.Rows.Count > 0) {
        // string test = double.Parse(availabilityTable.Rows[0]["Total2xx"].ToString()).ToString();
        // res.AddInsight(InsightStatus.Info, test);
        // double availabilityPercNum = 100 * (double) availabilityTable.Rows[0]["Availabilty"];
        //double total2xxNum = (double) availabilityTable.Rows[0]["Total2xx"];
        //double total4xxNum = (double) availabilityTable.Rows[0]["Total4xx"];
        //double total5xxNum = (double) availabilityTable.Rows[0]["Total5xx"];
        //double p50Num = (double) availabilityTable.Rows[0]["PerformanceP50"];
        //double p90Num = (double) availabilityTable.Rows[0]["PerformanceP90"];

        //string availabilityPerc = availabilityPercNum.ToString() + "%";
        //string total2xx = total2xxNum.ToString();
        //string total4xx = total4xxNum.ToString();
        //string total5xx = total5xxNum.ToString();
        //string p50 = p50Num.ToString() + "ms";
        //string p90 = p90Num.ToString() + "ms";
        
        // availabilityStatus;
        //SummaryCardStatus p90Status;
        //SummaryCardStatus p50Status;
        
        // if(availabilityPercNum > 99.9){
        //     availabilityStatus = SummaryCardStatus.Success;
        // }
        // else if (availabilityPercNum > 99){
        //     availabilityStatus = SummaryCardStatus.Warning;
        // }
        // else{
        //     availabilityStatus = SummaryCardStatus.Critical;
        // }

        //if(p50Num < 5){
        //    p50Status = SummaryCardStatus.Success;
        //}
        //else if (p50Num < 10){
        //    p50Status = SummaryCardStatus.Warning;
        //}
        //else{
        //    p50Status = SummaryCardStatus.Critical;
        //}

        //if(p50Num < 5){
        //    p90Status = SummaryCardStatus.Success;
        //}
        //else if (p50Num < 10){
        //    p90Status = SummaryCardStatus.Warning;
        //}
        //else{
        //    p90Status = SummaryCardStatus.Critical;
        //}
        
        // var availabliltyCard = new SummaryCard(
        //     title: "Availablilty",
        //     description: $@"Last {decimal.Parse(cxt["timeRange"])/24} days",
        //     status: availabilityStatus,
        //     message: availabilityPerc,
        //     onClickActionLink: "",
        //     onClickActionType:SummaryCardActionType.Detector
        // );
        // var summaryCards = new List<SummaryCard>();
        // summaryCards.Add(availabliltyCard);

        /*var total2xxCard = new SummaryCard(
            title: "Total 2xx",
            description: $@"Last {decimal.Parse(cxt["timeRange"])/24} days",
            status: SummaryCardStatus.Info,
            message: total2xx,
            onClickActionLink: "",
            onClickActionType:SummaryCardActionType.Detector
        );
        summaryCards.Add(total2xxCard);

        var total4xxCard = new SummaryCard(
            title: "Total 4xx",
            description: $@"Last {decimal.Parse(cxt["timeRange"])/24} days",
            status: SummaryCardStatus.Info,
            message: total4xx,
            onClickActionLink: "",
            onClickActionType:SummaryCardActionType.Detector
        );
        summaryCards.Add(total4xxCard);

        var total5xxCard = new SummaryCard(
            title: "Total 5xx",
            description: $@"Last {decimal.Parse(cxt["timeRange"])/24} days",
            status: SummaryCardStatus.Info,
            message: total5xx,
            onClickActionLink: "",
            onClickActionType:SummaryCardActionType.Detector
        );
        summaryCards.Add(total5xxCard);

        var p50Card = new SummaryCard(
            title: "Performance P50",
            description: $@"Last {decimal.Parse(cxt["timeRange"])/24} days",
            status: p50Status,
            message: p50,
            onClickActionLink: "",
            onClickActionType:SummaryCardActionType.Detector
        );
        summaryCards.Add(p50Card);

        var p90Card = new SummaryCard(
            title: "Performance P90",
            description: $@"Last {decimal.Parse(cxt["timeRange"])/24} days",
            status: p90Status,
            message: p90,
            onClickActionLink: "",
            onClickActionType:SummaryCardActionType.Detector
        );
        summaryCards.Add(p90Card);*/

        //res.AddSummaryCards(summaryCards);

        // DataTable graphData = await graphDataTask;
        // res.AddMarkdownView("graph");
        /*var Trends = new DiagnosticData(){
            Table = graphData,
    
            RenderingProperties = new TimeSeriesRendering(){
                Title = "Detector Availability Stats",
                GraphOptions = new {
                    yAxis = new {
                        title = new {
                            text = "Requests"
                        }
                    },
                    xAxis = new {
                        title = new {
                            text = "Time (UTC)"
                        }
                    }
                }
            }
        };

        res.Dataset.Add(Trends);*/
    
        DataTable exceptionsTable = await exceptionsTask;
        res.AddMarkdownView("exceptions");
        /*res.Dataset.Add(new DiagnosticData()
        {
            Table = exceptionsTable,
            RenderingProperties = new Rendering(RenderingType.Table){
                Title = "Exceptions",
                Description = "Top 10 Exceptions"
            }
        });*/
    //}

    /*else {
        res.AddInsight(InsightStatus.Info, "No requests found in the given timeframe");
    }*/

    //throw new Exception("test exception");

    }
    catch (Exception Ex){
        res.AddMarkdownView(Ex.ToString());
    }
    return res;
}
