using System;
using System.Threading;

[ArmResourceFilter(provider: "Microsoft.AppPlatform", resourceTypeName: "Spring")]
[Definition(Id = "darrelTestGist2", Name = "Return of the Test Gist", Author = "darreldonald", Description = "second test gist")]
public static class darrelTestGist2
{
    public static string test(){
        return "this is the second test :D";
    }

}