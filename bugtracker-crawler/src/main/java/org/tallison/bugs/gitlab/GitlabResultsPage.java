package org.tallison.bugs.gitlab;

import java.util.ArrayList;
import java.util.List;

public class GitlabResultsPage {

    int maxPage = -1;
    List<String> urls = new ArrayList<>();


    @Override
    public String toString() {
        return "GitlabResultsPage{" +
                "maxPage=" + maxPage +
                ", urls=" + urls +
                '}';
    }
}
