/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.bugs;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SubdirectoryMover {

    static int maxPerDirectory = 10000;

    public static void main(String[] args) throws Exception {
        Matcher m = Pattern.compile("(?i)[-_a-z]+-(\\d+)").matcher("");
        Path dir = Paths.get(args[0]);
        Map<Integer, List<Path>> filesByIssue = new TreeMap<>();
        int totalFiles = 0;
        for (File f : dir.toFile().listFiles()) {
            String fName = f.getName();

            int issueId = -1;
            if (m.reset(fName).find()) {
                issueId = Integer.parseInt(m.group(1));
            }
            if (issueId > -1) {
                List<Path> paths = filesByIssue.get(issueId);
                if (paths == null) {
                    paths = new ArrayList<>();
                    filesByIssue.put(issueId, paths);
                }
                paths.add(f.toPath());
            }
            totalFiles++;
        }
        if (totalFiles < maxPerDirectory) {
            return;
        }

        Map<Integer, Integer> issueRanges = new TreeMap<>();
        int currBin = 0;
        int currStart = -1;
        int lastIssue = -1;
        for (Integer issueid : filesByIssue.keySet()) {
            if (currStart < 0) {
                currStart = issueid;
                lastIssue = issueid;
            }
            int paths = filesByIssue.get(issueid).size();
            if (currBin+paths > maxPerDirectory) {
                issueRanges.put(currStart, lastIssue);
                currBin = paths;
                currStart = issueid;
            }
            currBin += paths;
            lastIssue = issueid;
        }
        System.out.println("LAST "+currStart + " " + lastIssue);
        issueRanges.put(currStart, lastIssue);
        for (int start : issueRanges.keySet()) {
            Path parent = filesByIssue.get(start).get(0).getParent();
            Path subdir = parent.resolve(start +"-"+issueRanges.get(start));
            Files.createDirectories(subdir);
            int cnt = 0;
            int end = issueRanges.get(start);
            for (int i = start; i <= end; i++) {
                if (filesByIssue.containsKey(i)) {
                    cnt += filesByIssue.get(i).size();
                    //System.out.println(i + " "+filesByIssue.get(i).size());
                    for (Path file : filesByIssue.get(i)) {
                        Path targ = subdir.resolve(file.getFileName());
                        System.out.println("moving "+file.toAbsolutePath()
                                +"->"+targ.toAbsolutePath());
                        Files.move(file, targ, StandardCopyOption.ATOMIC_MOVE);
                    }
                }
            }
            System.out.println(start +"-"+end + " "+cnt);
        }
    }
}
