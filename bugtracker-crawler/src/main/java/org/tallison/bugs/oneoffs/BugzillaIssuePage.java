package org.tallison.bugs.oneoffs;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class BugzillaIssuePage {

    //parallel arrays of attachment urls, extensions
    List<String> attachmentUrls = new ArrayList<>();
    List<String> extensions = new ArrayList<>();
    Instant reportedDate = null;

    @Override
    public String toString() {
        return "BugzillaIssuePage{" +
                "attachmentUrls=" + attachmentUrls +
                ", reportedDate=" + reportedDate +
                '}';
    }
}
