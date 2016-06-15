package com.sunteng.flume.source.tail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;

public class FileMatcher {
    private static final Logger logger = LoggerFactory
            .getLogger(FileMatcher.class);

    public static File getDirectory(String name) {
        if (name.contains("*")) {
            name = name.split("\\*")[0];
        }
        if (name.endsWith("/")) {
            name = name.substring(0, name.length() - 1);
        } else {
            name = new File(name).getParent();
        }
        return new File(name);
    }

    public static File[] listFiles(String matchPattern, final FinishMarker finishMarker) throws IOException {
        String[] splits = matchPattern.split("\\*");
        File directory = getDirectory(splits[0]);
        final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + matchPattern);
        final ArrayList<File> list = new ArrayList<File>();
        Files.walkFileTree(directory.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (matcher.matches(file) && !finishMarker.isExist(file.toString())) {
                    list.add(file.toFile());
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return list.toArray(new File[]{});
    }
}
