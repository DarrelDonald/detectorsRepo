using System;
using System.Threading;

[ArmResourceFilter(provider: "Microsoft.AppPlatform", resourceTypeName: "Spring")]
[Definition(Id = "darrelTestGist", Name = "darrel test gist", Author = "darreldonald", Description = "test21")]
public static class darrelTestGist
{
    public static string test(){
        return "this is a test";
    }

}