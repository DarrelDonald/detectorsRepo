[AppFilter (AppType = AppType.All, PlatformType = PlatformType.Windows, StackType = StackType.All, InternalOnly=true)]
[Definition(Id = "testNoCodeId", Name = "detectorName", Author = "darreldonald", Description = "describe yourself in 150 characters or fewer", Category = "NoCodeSupportingDLL")]
public static class testNoCodeId_NoCode_Class{
    public static string GetfirstQueryQuery(OperationContext<App> cxt)
    {
        return
        $@"print Title = 'issa title', MarkdownString = 'issa markdown string'";
    }


}
