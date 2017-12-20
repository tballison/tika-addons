package org.tallison.tika.unravelers;

public class RecursiveRoot {
    public static RecursiveRoot DEFAULT = new RecursiveRoot("/");

    private final String path;
    public RecursiveRoot(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }
}
