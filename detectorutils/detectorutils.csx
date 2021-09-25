using System.Text;
using System.Linq;
using System.Collections;

//2

[AppFilter]
[Definition(Id = "DetectorUtils", Name = "DetectorUtils", Author = "puneetg", Description = "A simple class that contains utility functions for most common detector tasks")]
public static class DetectorUtils {

    public static string DataTableToMarkdown(DataTable dt)
    {
        if (dt.Rows.Count > 0)
        {
            var markDownBuilder = new StringBuilder();
            List<string> columns = new List<string>();

            markDownBuilder.AppendLine(" <table>");
            markDownBuilder.AppendLine(" <tr>");
            foreach(DataColumn c in dt.Columns)
            {
                markDownBuilder.Append($" <th> {c.ColumnName} </th>");
            }
            markDownBuilder.AppendLine(" </tr>");

            
            foreach (DataRow dr in dt.Rows)
            {
                markDownBuilder.AppendLine(" <tr>");
                markDownBuilder.AppendLine(string.Join(" ", dr.ItemArray.Select(cell => $" <td> {cell} </td>")));
                markDownBuilder.AppendLine(" </tr>");
            }
             markDownBuilder.AppendLine(" </table>");

            return markDownBuilder.ToString();
        }
        else
        {
            return string.Empty;
        }
    }

    public static string DictionaryToMarkdown(Dictionary<string, string> dict, string column1, string column2)
    {
        if (dict.Any())
        {
            var markDownBuilder = new StringBuilder();
            markDownBuilder.AppendLine($" {column1} | {column2} ");
            markDownBuilder.AppendLine(" --- | --- ");
            foreach (KeyValuePair<string, string> kvp in dict)
            {
                markDownBuilder.AppendLine($" {kvp.Key} | {kvp.Value}");
            }
            return markDownBuilder.ToString();
        }
        else
        {
            return string.Empty;
        }
    }

    public static string GetApplensUrl(OperationContext<App> cxt)
    {
        var hostingEnv = cxt.Resource.Stamp;
        string stampName = !string.IsNullOrWhiteSpace(hostingEnv.InternalName) ? hostingEnv.InternalName : hostingEnv.Name;

        string applensUrl = "applens.azurewebsites.net";

        if(stampName.StartsWith("cnws", StringComparison.CurrentCultureIgnoreCase)){
            applensUrl = "applens.chinacloudsites.cn";
        }else if(stampName.StartsWith("gcws", StringComparison.CurrentCultureIgnoreCase)){
            applensUrl = "applens.azurewebsites.us";
        }

        return applensUrl;
    }    

    public static string GetDetectorLink(OperationContext<App> cxt, string routePath, string text, bool openInNewTab = false)
    {
        string href= "";
        string style = "color:#3c8dbc;border:1px solid #3c8dbc;text-align: center;border-radius: 5px; padding:3px;";
        string targetAttribute = "";
        if (cxt.IsInternalCall)
        {
            var startTime = DateTime.ParseExact(cxt.StartTime, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);
            var endTime = DateTime.ParseExact(cxt.EndTime, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);

            var startTimeSting = startTime.ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture);
            var endTimeString = endTime.ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture);

            if (openInNewTab)
            {
                href= $"https://{GetApplensUrl(cxt)}{cxt.Resource.ResourceUri}/{routePath}?startTime={startTimeSting}&endTime={endTimeString}";
                targetAttribute = " target = '_blank' ";
            }
            else
            {
                href= $"{cxt.Resource.ResourceUri}/{routePath}";
            }
        }
        else
        {
            href= $"./resource/{cxt.Resource.ResourceUri.ToLower()}/{routePath}";
        }

        return $"<a href='{href}' {targetAttribute} style='{style}'><i aria-hidden='true' class='fa fa-stethoscope'></i> {text}</a>";

    }

    public static string GetDetectorLinkWithQueryParams(OperationContext<App> cxt, string routePath, string text, string queryParams = "", bool openInNewTab = false)
    {
        string href= "";
        string style = "color:#3c8dbc;border:1px solid #3c8dbc;text-align: center;border-radius: 5px; padding:3px;";
        string targetAttribute = "";
        if (cxt.IsInternalCall)
        {
            var startTime = DateTime.ParseExact(cxt.StartTime, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);
            var endTime = DateTime.ParseExact(cxt.EndTime, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);

            var startTimeSting = startTime.ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture);
            var endTimeString = endTime.ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture);

            if (openInNewTab)
            {
                href= $"https://{GetApplensUrl(cxt)}{cxt.Resource.ResourceUri}/{routePath}?startTime={startTimeSting}&endTime={endTimeString}{queryParams}";
                targetAttribute = " target = '_blank' ";
            }
            else
            {
                href= $"{cxt.Resource.ResourceUri}/{routePath}";
            }
        }
        else
        {
            href= $"./resource/{cxt.Resource.ResourceUri.ToLower()}/{routePath}";
        }

        return $"<a href='{href}' {targetAttribute} style='{style}'><i aria-hidden='true' class='fa fa-stethoscope'></i> {text}</a>";

    }

    public static string GetDetectorLink(OperationContext<App> cxt, string routePath, string text, string slot, bool openInNewTab = false)
    {
        string href= "";
        string style = "color:#3c8dbc;border:1px solid #3c8dbc;text-align: center;border-radius: 5px; padding:3px;";
        string targetAttribute = "";
        string resourceUri = string.Empty;
        
        if(slot.Equals("Production", StringComparison.CurrentCultureIgnoreCase))
            resourceUri = $"subscriptions/{cxt.Resource.SubscriptionId}/resourceGroups/{cxt.Resource.ResourceGroup}/providers/{cxt.Resource.Provider}/sites/{cxt.Resource.Name}";
        else
            resourceUri = $"subscriptions/{cxt.Resource.SubscriptionId}/resourceGroups/{cxt.Resource.ResourceGroup}/providers/{cxt.Resource.Provider}/sites/{cxt.Resource.Name}({slot})";

        if (cxt.IsInternalCall)
        {
            var startTime = DateTime.ParseExact(cxt.StartTime, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);
            var endTime = DateTime.ParseExact(cxt.EndTime, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);

            var startTimeSting = startTime.ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture);
            var endTimeString = endTime.ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture);
            

            if (openInNewTab)
            {
                href= $"https://{GetApplensUrl(cxt)}/{resourceUri}/{routePath}?startTime={startTimeSting}&endTime={endTimeString}";
                targetAttribute = " target = '_blank' ";
            }
            else
            {
                href= $"{resourceUri}/{routePath}";
            }
        }
        else
        {
            //Always open in new tab
            resourceUri = $"subscriptions/{cxt.Resource.SubscriptionId}/resourceGroups/{cxt.Resource.ResourceGroup}/providers/{cxt.Resource.Provider}/sites/{cxt.Resource.Name}/slots/{slot}";
            href= $"https://portal.azure.com/?websitesextension_ext=asd.featurePath%3D{routePath}#resource/{resourceUri.ToLower()}/troubleshoot";
            targetAttribute = " target = '_blank' ";
        }

        return $"<a href='{href}' {targetAttribute} style='{style}'><i aria-hidden='true' class='fa fa-stethoscope'></i> {text}</a>";

    }

    public static void RetryOnException(Action operation, TimeSpan delay, int times = 3)
    {
        var attempts = 0;
        do
        {
            try
            {
                attempts++;
                operation();
                break; // Sucess! Lets exit the loop!
            }
            catch (Exception)
            {
                if (attempts == times)
                {
                    throw;
                }
                Task.Delay(delay).Wait();
            }
        } while (true);
    }

    public static async Task RetryOnExceptionAsync(
            int times, TimeSpan delay, Func<Task> operation)
        {
            await RetryOnExceptionAsync<Exception>(times, delay, operation);
        }

        public static async Task RetryOnExceptionAsync<TException>(
            int times, TimeSpan delay, Func<Task> operation) where TException : Exception
        {
            if (times <= 0) 
                throw new ArgumentOutOfRangeException(nameof(times));

            var attempts = 0;
            do
            {
                try
                {
                    attempts++;
                    await operation();
                    break;
                }
                catch (TException)
                {
                    if (attempts == times)
                        throw;

                    await Task.Delay(delay);
                }
            } while (true);
        }

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
    }

    public static List<Solution> RemoveDuplicateSolutions(List<Solution> solutions)
    {
        return solutions.Distinct(new SolutionComparer()).ToList();
 
    }

    class SolutionComparer : IEqualityComparer<Solution>
    {
        public bool Equals(Solution x, Solution y)
        {
            return x.Name.Equals(y.Name);
        }

        public int GetHashCode(Solution obj)
        {
            return obj.Name.GetHashCode();
        }
    }

}
