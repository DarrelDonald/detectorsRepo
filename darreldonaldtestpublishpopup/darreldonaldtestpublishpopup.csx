using System;
using System.Threading;

[ArmResourceFilter(provider: "Microsoft.AppPlatform", resourceTypeName: "Spring")]
[Definition(Id = "darreldonaldTestPublishPopup", Name = "test", Author = "darreldonald", Description = "test")]
public async static Task<Response> Run(DataProviders dp, OperationContext<ArmResource> cxt, Response res)
{
    res.AddInsight(InsightStatus.Info, "test");

    return res;
}