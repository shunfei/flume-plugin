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
import com.google.common.base.Throwables;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.SpoolDirectorySource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TailSource extends AbstractSource
        implements EventDrivenSource, Configurable {

    public static final String CONF_FILE_PATH = "filePath";

    public static final String CONF_DESERIALIZER = "deserializer";
    public static final String DEFAULT_DESERIALIZER = "LINE";

    public static final String CONF_BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 100;

    public static final String CONF_MAX_BACKOFF = "maxBackoff";
    public static final int DEFAULT_MAX_BACKOFF = 4000;

    public static final String CONF_INPUT_CHARSET = "inputCharset";
    public static final String DEFAULT_INPUT_CHARSET = "UTF-8";

    public static final String CONF_META_DIRECTORY = "metaDirectory";
    public static final String DEFAULT_META_DIRECTORY = ".";

    public static final String CONF_RATE_LIMIT = "rateLimit";
    public static final String DEFAULT_CONF_RATE_LIMIT = "/etc/flume_rate_limit.conf";
    public static final String CONF_RATE_LIMIT_REFRESH = "rateLimitRefresh";
    public static final int DEFAULT_CONF_RATE_LIMIT_REFRESH = 60;

    public static final String CONF_SCAN_ON_BEGIN = "scanOnBegin";
    public static final boolean DEFAULT_CONF_SCAN_ON_BEGIN = false;

    /**
     * What to do when there is a character set decoding error.
     */
    public static final String CONF_DECODE_ERROR_POLICY = "decodeErrorPolicy";
    public static final String DEFAULT_DECODE_ERROR_POLICY =
            DecodeErrorPolicy.FAIL.name();


    private static final Logger logger = LoggerFactory
            .getLogger(SpoolDirectorySource.class);
    private static final int POLL_DELAY_MS = 1000;
    private SourceCounter sourceCounter;

    private int batchSize;
    private String fileMatchPattern;
    private String pathPattern;
    private File metaDirectory;
    private File directory;
    private boolean backoff = true;
    private int maxBackoff;
    private String deserializerType;
    private Context deserializerContext;
    private ScheduledExecutorService executor;
    private TailFileEventReader reader;
    private String inputCharset;
    private DecodeErrorPolicy decodeErrorPolicy;
    private TailRateLimit rateLimit;
    private FinishMarker finishMarker;

    @Override
    public void configure(Context context) {
        String pathPattern = context.getString(CONF_FILE_PATH);
        Preconditions.checkState(pathPattern != null, "Configuration must specify a file path");
        this.pathPattern = pathPattern;
        directory = FileMatcher.getDirectory(pathPattern);
        fileMatchPattern = Paths.get(pathPattern).getFileName().toString();

        deserializerType = context.getString(CONF_DESERIALIZER, DEFAULT_DESERIALIZER);
        deserializerContext = new Context(context.getSubProperties(CONF_DESERIALIZER + "."));

        batchSize = context.getInteger(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        maxBackoff = context.getInteger(CONF_MAX_BACKOFF, DEFAULT_MAX_BACKOFF);

        inputCharset = context.getString(CONF_INPUT_CHARSET, DEFAULT_INPUT_CHARSET);
        decodeErrorPolicy = DecodeErrorPolicy.valueOf(context
                .getString(CONF_DECODE_ERROR_POLICY, DEFAULT_DECODE_ERROR_POLICY).toUpperCase());

        metaDirectory = new File(context.getString(CONF_META_DIRECTORY, DEFAULT_META_DIRECTORY));


        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }

        boolean scanOnBegin = context.getBoolean(CONF_SCAN_ON_BEGIN, DEFAULT_CONF_SCAN_ON_BEGIN);
        File markerPath = new File(metaDirectory, patternEncode(pathPattern, "marker"));
        try {
            this.finishMarker = new FinishMarker(markerPath);

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e.getMessage());
        }

        try {
            finishMarker.compact();
        } catch (IOException e) {
            e.printStackTrace();
        }

        reader = new TailFileEventReader(pathPattern, metaDirectory, fileMatchPattern,
                deserializerType, deserializerContext,
                decodeErrorPolicy, inputCharset, scanOnBegin, finishMarker);

        int rateLimitRefresh = context.getInteger(CONF_RATE_LIMIT_REFRESH, DEFAULT_CONF_RATE_LIMIT_REFRESH);
        rateLimit = new TailRateLimit(rateLimitRefresh, new File(context.getString(CONF_RATE_LIMIT, DEFAULT_CONF_RATE_LIMIT)));
    }

    public String patternEncode(String pattern, String suffix ) {
        return Base64.getEncoder().encodeToString(pattern.getBytes()) + "." + suffix;
    }

    @Override
    public synchronized void start() {
        logger.info("TailSource start with directory: {}, matchPattern: {}",
                directory, pathPattern);
        rateLimit.start();
        Runnable runner = new TailSourceRunnable(reader);
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleWithFixedDelay(runner, 0, POLL_DELAY_MS, TimeUnit.MILLISECONDS);

        ScheduledExecutorService markerExecutor = Executors.newSingleThreadScheduledExecutor();
        markerExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    finishMarker.compact();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 0, 1, TimeUnit.DAYS);

        super.start();
        logger.debug("TailSource started");
        sourceCounter.start();
    }

    @Override
    public synchronized void stop() {
        executor.shutdown();
        try {
            executor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            logger.info("Interrupted while awaiting termination", ex);
        }
        executor.shutdownNow();

        super.stop();
        sourceCounter.stop();
        logger.info("TailSource source {} stopped. Metrics: {}",
                getName(), sourceCounter);

        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "Tail source " + getName() + ": { filePath: " + directory + " }";
    }

    private class TailSourceRunnable implements Runnable {
        private TailFileEventReader reader;

        private TailSourceRunnable(TailFileEventReader reader) {
            this.reader = reader;
        }

        private long getAllSize(List<Event> events) {
            long size = 0;
            for (Event e: events) {
                size += e.getBody().length;
            }
            return size;
        }

        @Override
        public void run() {
            int backoffInterval = 100;

            try {
                while (!Thread.interrupted()) {
                    List<Event> events = reader.readEvents(batchSize);
                    if (events.isEmpty()) {
                        return;
                    }

                    while (!rateLimit.canProcess(getAllSize(events))) {
                        TimeUnit.MILLISECONDS.sleep(250);
                    }

                    sourceCounter.addToEventReceivedCount(events.size());
                    sourceCounter.incrementAppendBatchReceivedCount();

                    try {
                        getChannelProcessor().processEventBatch(events);
                        reader.commit();
                    } catch (ChannelException ex) {
                        logger.warn("The channel is full, and cannot write data now. The " +
                                "source will try again after " + String.valueOf(backoffInterval) +
                                " milliseconds:" + ex.getMessage());
                        logger.info("fail events:" + events.size());
                        if (backoff) {
                            TimeUnit.MILLISECONDS.sleep(backoffInterval);
                            backoffInterval = backoffInterval << 1;
                            backoffInterval = backoffInterval >= maxBackoff ? maxBackoff :
                                    backoffInterval;
                        }
                        continue;
                    }
                    backoffInterval = 250;
                    sourceCounter.addToEventAcceptedCount(events.size());
                    sourceCounter.incrementAppendBatchAcceptedCount();
                }
                logger.info("Tail Source runner has shutdown.");
                TimeUnit.MILLISECONDS.sleep(backoffInterval);
            } catch (Throwable t) {
                logger.error("FATAL: " + TailSource.this.toString() + ": " +
                        "Uncaught exception in TailSource thread. " +
                        "Restart or reconfigure Flume to continue processing.", t);
                Throwables.propagate(t);
            } finally {
                // read all file if rotate?
                // logger.info("quit, reader:" + reader.rotated());
            }

        }
    }

}
