
[AppFilter]
[Definition(Id = "darreldonaldTestGist", Name = "testGist", Author = "b-darreldonald", Description = "test gist")]
public static class testGist {
    const string version = "version 5";

    public static string getVersion(){
        return version;
    }
}