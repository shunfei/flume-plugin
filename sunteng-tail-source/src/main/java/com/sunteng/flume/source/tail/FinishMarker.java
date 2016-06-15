package com.sunteng.flume.source.tail;

import org.apache.commons.codec.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;

public class FinishMarker {
    private static final Logger logger = LoggerFactory
            .getLogger(FinishMarker.class);

    public File target;
    public BufferedWriter writer;
    private Object lock;
    private ArrayList<String> fileList;

    public FinishMarker(File target) throws FileNotFoundException {
        this.lock = this;
        this.target = target;
        this.writer = getWriter(target, true);
        this.fileList = new ArrayList<String>();
        loadFromDisk();
    }

    public int size() {
        return fileList.size();
    }

    private BufferedReader getReader() throws FileNotFoundException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(target), Charsets.UTF_8));
    }

    public BufferedWriter getWriter(File target, boolean append) throws FileNotFoundException {
        return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(target, append), Charsets.UTF_8));
    }

    private void loadFromDisk() {
        ArrayList<String> fileList = new ArrayList<String>();
        BufferedReader reader;
        synchronized (lock) {
            try {
                reader = getReader();
            } catch (IOException ignored) {
                return;
            }

            while (true) {
                String filePath;
                try {
                    filePath = reader.readLine();
                } catch (IOException e) {
                    break;
                }
                if (filePath == null) break;
                fileList.add(filePath);
            }
            Collections.sort(fileList);
            this.fileList = fileList;

            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            logger.info("FinishMark: {}", fileList);
        }
    }

    public void mark(String filePath) throws IOException {
        synchronized (lock) {
            this.writer.append(filePath);
            this.writer.append("\n");
            this.writer.flush();
            fileList.add(filePath);
            Collections.sort(fileList);
        }
    }

    public int find(String fileName) {
        // fileList would be sorted
        return Collections.binarySearch(fileList, fileName);
    }

    public boolean isExist(String fileName) {
        return find(fileName) >= 0;
    }

    private void closeSafe() {
        try {
            this.writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        synchronized (lock) {
            closeSafe();
        }
    }

    public void compact() throws IOException {
        synchronized (lock) {
            ArrayList<String> newFileList = new ArrayList<String>();
            for (String filePath : fileList) {
                if (Files.exists(Paths.get(filePath))) {
                    newFileList.add(filePath);
                }
            }
            logger.info("finishMarker compact: " + newFileList.size() + ", realSize:" + fileList.size());
            if (newFileList.size() == fileList.size()) {
                // nothing changed;
                return;
            }

            File tmpTarget = new File(target.getPath() + ".tmp");
            tmpTarget.delete();
            BufferedWriter writer = getWriter(tmpTarget, false);
            for (String filePath : newFileList) {
                writer.append(filePath);
                writer.append("\n");
            }
            writer.flush();

            closeSafe();
            Files.move(tmpTarget.toPath(), target.toPath(),
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);

            this.fileList = newFileList;
            this.writer = getWriter(target, true);
        }
    }
}
