package org.tallison.bugs.gitlab;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class GitlabIssue {
    List<String> attachedUrls = new ArrayList<>();
    List<String> externalUrls = new ArrayList<>();
    Instant opened;

    @Override
    public String toString() {
        return "GitlabIssue{" +
                "attachedUrls=" + attachedUrls +
                ", externalUrls=" + externalUrls +
                ", opened=" + opened +
                '}';
    }
}
