#load "darrelTestGist"
using System;
using System.Threading;

[ArmResourceFilter(provider: "Microsoft.AppPlatform", resourceTypeName: "Spring")]
[Definition(Id = "darreltestdetector", Name = "darrel test detector", Author = "darreldonald", Description = "test")]
public async static Task<Response> Run(DataProviders dp, OperationContext<ArmResource> cxt, Response res)
{
    res.AddInsight(InsightStatus.Info, darrelTestGist.test());
    

    return res;
}