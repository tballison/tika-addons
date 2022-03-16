package org.tallison.bugs.trac;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TracIssuePage {

    private int issueId;
    private String nextLink;
    private String zipLink;
    private Set<String> linkedAttachments = new HashSet<>();
    private boolean alreadyCrawled = false;
    public TracIssuePage(boolean alreadyCrawled) {
        this.alreadyCrawled = alreadyCrawled;
    }
    public int getIssueId() {
        return issueId;
    }

    public void setIssueId(int issueId) {
        this.issueId = issueId;
    }

    public String getZipLink() {
        return zipLink;
    }

    public void setZipLink(String zipLink) {
        this.zipLink = zipLink;
    }

    public boolean hasZipLink() {
        return zipLink != null;
    }

    public void setNextLink(String nextLink) {
        this.nextLink = nextLink;
    }

    public String getNextLink() {
        return nextLink;
    }

    public Set<String> getExternalLinks() {
        return linkedAttachments;
    }

    public void addExternalLink(String externalLink) {
        linkedAttachments.add(externalLink);
    }

    public boolean isAlreadyCrawled() {
        return alreadyCrawled;
    }



    @Override
    public String toString() {
        return "TracIssuePage{" + "issueId=" + issueId + ", nextLink='" + nextLink + '\'' +
                ", zipLink='" + zipLink + '\'' + ", linkedAttachments=" + linkedAttachments +
                ", alreadyCrawled=" + alreadyCrawled + '}';
    }
}
