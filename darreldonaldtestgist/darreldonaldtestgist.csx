
[AppFilter]
[Definition(Id = "darreldonaldTestGist", Name = "testGist", Author = "a-darreldonald", Description = "test gist")]
public static class testGist {
    const string version = "version 3";

    public static string getVersion(){
        return version;
    }
}