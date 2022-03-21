using System;
using System.Threading;

[ArmResourceFilter(provider: "Microsoft.AppPlatform", resourceTypeName: "Spring")]
[Definition(Id = "darrelTestGist", Name = "darrel test gist", Author = "darreldonald", Description = "test")]
public static class darrelTestGist
{
    static string test(){
        return "this is a test";
    }

}