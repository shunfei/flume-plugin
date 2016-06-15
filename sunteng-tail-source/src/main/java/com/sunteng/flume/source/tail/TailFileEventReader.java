/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.sunteng.flume.source.tail;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Base64;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.EventDeserializerFactory;
import org.apache.flume.serialization.ResettableFileInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TailFileEventReader implements ReliableEventReader {
    private static final Logger logger = LoggerFactory
            .getLogger(TailFileEventReader.class);

    private FinishMarker finishMarker;

    private String deserializerType;
    private Context deserializerContext;

    private final Charset inputCharset;
    private final DecodeErrorPolicy decodeErrorPolicy;

    private String pattern;
    private String fileMatchPattern;
    private FileReader curFileReader;
    private File metaDirectory;
    private File metaFile;
    private File cleanLockFile; // to determine whether if TailSource first run
    private boolean scanAllOnBegin;
    private boolean isSingleRotate;
    private boolean committed = true;

    public TailFileEventReader(String pattern, File metaDirectory, String fileMatchPattern,
                               String deserializerType, Context deserializerContext,
                               DecodeErrorPolicy decodeErrorPolicy,
                               String inputCharset, boolean scanAllOnBegin, FinishMarker finishMarker
    ) {
        this.deserializerContext = deserializerContext;
        this.metaDirectory = metaDirectory;
        this.deserializerType = deserializerType;
        this.inputCharset = Charset.forName(inputCharset);
        this.decodeErrorPolicy = Preconditions.checkNotNull(decodeErrorPolicy);
        this.pattern = pattern;
        this.fileMatchPattern = fileMatchPattern;
        this.scanAllOnBegin = scanAllOnBegin;
        this.finishMarker = finishMarker;
        isSingleRotate = !fileMatchPattern.contains("*");
        metaFile = metaNameEncoding(pattern);
        cleanLockFile = cleanLockFileEncoding(pattern);
    }

    private File cleanLockFileEncoding(String pattern) {
        String filePath = new String(Base64.encodeBase64(pattern.getBytes())) + ".cleanlock";
        return new File(metaDirectory, filePath);
    }

    private File metaNameEncoding(String pattern) {
        String filePath = new String(Base64.encodeBase64(pattern.getBytes())) + ".meta";
        return new File(metaDirectory, filePath);
    }

    @Override
    public void commit() throws IOException {
        if (!committed && curFileReader != null) {
            committed = true;
            curFileReader.commit();
        }
    }

    @Override
    public Event readEvent() throws IOException {
        throw new IllegalStateException("readEvent not supported yet");
    }

    public boolean rotated() {
        if (curFileReader != null) {
            return curFileReader.rotated;
        }
        return false;
    }

    int lastEmpty = 0;

    @Override
    public List<Event> readEvents(int numEvents) throws IOException {

        while (true) {
            if (!committed) {
                if (curFileReader == null) {
                    throw new IllegalStateException("File should not roll when commit is outstanding.");
                }
                logger.debug("Last read was never committed - resetting mark position.");
                curFileReader.reset();
            } else {
                // Check if new files have arrived since last call
                if (curFileReader == null) {
                    curFileReader = selectOpenFiles();
                }
                // couldn't find any available file
                if (curFileReader == null) {
                    return Collections.emptyList();
                }
            }

            List<Event> events = curFileReader.readEvents(numEvents);

            if (events.isEmpty()) { // eof
                File next = getEarliestFile(curFileReader.file);
                logger.info("next file: " + next +  ", current file: " + curFileReader.file);
                if (curFileReader.isNeedClean(next)) { // I think I will have a new file
                    logger.info("clean meta file");
                    try {
                        cleanCurrentFile();
                    } catch (IOException ignored) {
                        // catch and try next time.
                    }
                    continue;
                }

                logger.debug("no need to clean, return empty: " + lastEmpty);
                lastEmpty++;
                return Collections.emptyList();
            } else {
                if (lastEmpty > 0) {
                    logger.debug("lastEmpty: " + lastEmpty, "now finally got one");
                    lastEmpty = 0;
                }
            }

            committed = false;
            return events;
        }
    }

    private void cleanCurrentFile() throws IOException {
        if (curFileReader == null) return;
        if (!isSingleRotate) {
            curFileReader.markFinish();
        }

        deleteFile(metaFile);
        try {
            curFileReader.close();
        } catch (IOException ignored) {}
        curFileReader = null;
    }

    @Override
    public void close() throws IOException {
        if (curFileReader != null) {
            curFileReader.close();
        }
    }

    private static class FileReader {
        private final File file;
        private long prevOffset;
        private long currentOffset;
        private String currentFileKey;
        private final EventDeserializer deserializer;
        private boolean rotated;
        private Event lastEvent;
        private String lastEventIno;
        private boolean isSingleRotate;
        private DurablePositionTrack tracker;
        private ResettableFileInputStream in;
        private FinishMarker finishMarker;

        public FileReader(File file, EventDeserializer deserializer,
                          ResettableFileInputStream in, DurablePositionTrack tracker,
                          boolean isSingleRotate, FinishMarker finishMarker) {
            this.file = file;
            this.in = in;
            this.isSingleRotate = isSingleRotate;
            this.tracker = tracker;
            this.deserializer = deserializer;
            this.currentFileKey = getFileKey(file);
            this.finishMarker = finishMarker;
        }

        public List<Event> readEvents(int numEvents) throws IOException {
            List<Event> ret = deserializer.readEvents(numEvents);
            if (!ret.isEmpty()) {
                if (lastEvent == null) {
                    logger.debug("first event:" + new String(ret.get(0).getBody()) + "; ino:" + tracker.getIno());
                }

                lastEvent = ret.get(ret.size() - 1);
                lastEventIno = tracker.getIno();
            }
            if (isSingleRotate) {
                prevOffset = currentOffset;
                currentOffset = file.length();
                if (currentOffset < prevOffset) {
                    rotated = true;
                }
            }
            // logger.info("readEvent: prev:" + prevOffset + ", current:" + currentOffset);
            return ret;
        }


        // decide whether clean the meta file when we reach eof(got empty events)
        public boolean isNeedClean(File nextFile) {
            // size increase
            if (isSingleRotate && !rotated) {
                logger.debug("not rotated!, not clean: " + currentOffset + "," + prevOffset);
                return false;
            }

            // watch file not exists
            if (nextFile == null || !nextFile.exists()) {
                logger.debug("file.exists(), not clean:" + currentOffset + "," + prevOffset);
                return false;
            }

            if (getFileKey(nextFile).equals(currentFileKey)) {
                logger.debug("currentOffset == prevOffset: " +
                        "newFileKey.equals(currentFileKey):");
                return false;
            }

            return true;
        }

        public void reset() throws IOException {
            deserializer.reset();
        }

        public void commit() throws IOException {
            deserializer.mark();
        }

        public void markFinish() throws IOException {
            finishMarker.mark(file.getPath());
        }

        public void close() throws IOException {
            if (lastEvent != null) {
                logger.debug("last event:" + new String(lastEvent.getBody()) + "; ino:" + lastEventIno);
            }
            logger.debug("in last pos:" + in.getMarkPosition());
            deserializer.close();
        }
    }

    public static String getFileKey(File file) {
        BasicFileAttributes basicAttr;
        try {
            basicAttr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
        } catch (IOException e) {
            return new String();
        }
        Object obj = basicAttr.fileKey();
        if (obj == null) {
            return new String();
        }
        return obj.toString();
    }

    private FileReader selectOpenFiles() {
        File earliestFile = getEarliestFile(null);
        if (earliestFile == null) {
            return null;
        }
        return openfile(earliestFile, metaFile, isSingleRotate);
    }

    private File getEarliestFile(File now) {
        if (isSingleRotate && now != null) {
            logger.info("getearliestfile:" + now);
            return now;
        }
        File[] files = new File[0];
        try {
            files = FileMatcher.listFiles(pattern, finishMarker);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (files.length == 0) {
            logger.warn("Could not find file");
            return null;
        }
        Arrays.sort(files);
        if (now == null) {
            return files[0];
        }
        logger.debug("selecting file:" + now);
        for (int i = 0; i < files.length; i++) {
            if (!files[i].equals(now)) {
                continue;
            }
            if (i + 1 < files.length) {
                return files[i + 1];
            }
            return null;
        }
        return null;
    }

    private FileReader openfile(File file, File metaFile, boolean isSingleRotate) {
        try {
            String currentPath = file.getPath();
            logger.info("try to openfile...");
            boolean exist = metaFile.exists();

            DurablePositionTrack tracker = DurablePositionTrack.getInstance(metaFile, currentPath);
            boolean change = !tracker.getTarget().equals(currentPath);
            if (!change) {
                String ino1 = tracker.getIno();
                String ino2 = getFileKey(new File(currentPath));
                logger.debug("event: not change, " + ino1 + "," + ino2 + ":" + exist);
                change = !ino1.equals(ino2);
            }
            if (change) {
                logger.debug("tracker rotate...(target not same)");
                tracker.close();
                deleteFile(metaFile);
                tracker = DurablePositionTrack.getInstance(metaFile, currentPath);
            }

            if (!tracker.isExists && !scanAllOnBegin && !cleanLockFile.exists()) {
                tracker.storePosition(file.length());
                // lock it
                cleanLockFile.createNewFile();
            }


            // sanity check
            Preconditions.checkState(tracker.getTarget().equals(currentPath),
                    "Tracker target %s does not equal expected filename %s",
                    tracker.getTarget(), currentPath);

            ResettableFileInputStream in = new ResettableFileInputStream(file, tracker,
                    ResettableFileInputStream.DEFAULT_BUF_SIZE, inputCharset, decodeErrorPolicy);
            logger.info("get last use of offset:" + in.getMarkPosition());
            EventDeserializer deserializer = EventDeserializerFactory.getInstance
                    (deserializerType, deserializerContext, in);

            return new FileReader(file, deserializer, in, tracker, isSingleRotate, finishMarker);
        } catch (FileNotFoundException e) {
            // File could have been deleted in the interim
            logger.warn("Could not find file: ", e);
            return null;
        } catch (IOException e) {
            logger.error("Exception opening file: " + file, e);
            return null;
        }
    }

    private void deleteFile(File metaFile) throws IOException {
        if (metaFile.exists() && !metaFile.delete()) {
            throw new IOException("Unable to delete old meta file " + metaFile);
        }
    }

}
