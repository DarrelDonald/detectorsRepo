#load "darreldonaldTestGist"
#load "geoUtilities"

//still works i hope

[AppFilter(AppType = AppType.WebApp, PlatformType = PlatformType.Windows, StackType = StackType.All)]
[Definition(Id = "darreldonaldRevampTest", Name = "revampTestDetector", Author = "darreldonald", Description = "detector to test for revamp")]
public async static Task<Response> Run(DataProviders dp, OperationContext<App> cxt, Response res)
{
    res.AddInsight(InsightStatus.Info, testGist.getVersion());

    return res;
}