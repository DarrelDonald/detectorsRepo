
[AppFilter]
[Definition(Id = "darreldonaldTestGist", Name = "testGist", Author = "darreldonald", Description = "test gist")]
public static class testGist {
    const string version = "version 2";

    public static string getVersion(){
        return version;
    }
}
