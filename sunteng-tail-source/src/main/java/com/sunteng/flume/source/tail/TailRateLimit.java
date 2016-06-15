package com.sunteng.flume.source.tail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * syntax:
 *   00:00 ~ 11:00 = 100mb
 *   2015/07/29.10:00 ~ 2015/07/30.03:00 = 100mb
 *   2015/07/29.10:00 ~ 2015/07/29.22:00 = 10mb
 */
public class TailRateLimit implements Runnable {
    private static final Logger logger = LoggerFactory
            .getLogger(TailRateLimit.class);

    private Pattern timeSegment = Pattern.compile(
            "^(\\d{2}):(\\d{2})\\s*~\\s*(\\d{2}):(\\d{2})\\s*=\\s*(.+)$");
    private Pattern dateSegment = Pattern.compile(
            "^(\\d{4}/\\d{2}/\\d{2}\\.\\d{2}:\\d{2})\\s*~\\s*(\\d{4}/\\d{2}/\\d{2}\\.\\d{2}:\\d{2})\\s*=\\s*(.+)$");


    private static long timeOffset = TimeZone.getDefault().getRawOffset();
    File file;
    byte[] cacheFileContent = {};
    Object lock = new Object();
    SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy/MM/dd.HH:mm");
    Hashtable<Integer, Long> baseMap;
    private AtomicBoolean init = new AtomicBoolean();
    private AtomicBoolean forceRefresh = new AtomicBoolean();
    private ScheduledExecutorService executor;
    private int refreshSec;


    public TailRateLimit(int refreshSec, File file) {
        this.file = file;
        this.refreshSec = refreshSec;
    }

    public void stop() {
        executor.shutdownNow();
    }

    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleWithFixedDelay(this, 0, refreshSec, TimeUnit.SECONDS);
    }

    public void run() {
        // logger.info("im processing");
        FileInputStream fis = null;
        try {
            fis = readFile();
            if (fis == null) {
                int fileLength = (int) file.length();
                FileInputStream tmp = new FileInputStream(file);
                cacheFileContent = new byte[fileLength];
                int byteRead = tmp.read(cacheFileContent);
                if (byteRead != fileLength) {
                    // retry next time
                    logger.info("the length of configure file is not expected, retry next time");
                    return;
                }
                tmp.close();
            }
        } catch (IOException e) {
            logger.debug("ratelimit file not found:" + e.getMessage());
            return;
        }
        try {
            processFile(fis);
        } catch (FileNotFoundException e) {
            logger.debug("ratelimit file not found: " + file.getPath());
            return;
        }
        init.set(true);
        forceRefresh.set(true);
    }

    private Long parseVal(String val) {
        val = val.toLowerCase().trim();
        long unit = 1;
        int sub = 2;
        if (val.endsWith("gb")) {
            unit <<= 30;
        } else if (val.endsWith("mb")) {
            unit <<= 20;
        } else if (val.endsWith("kb")) {
            unit <<= 10;
        } else if (val.endsWith("b")) {
            sub = 1;
        } else {
            sub = 0;
        }
        if (unit > 1) {
            val = val.substring(0, val.length() - sub).trim();
        }
        return Long.valueOf(val) * unit;
    }

    private int getTimeVal(String hour, String minute) {
        return Integer.parseInt(hour) * 60 + Integer.parseInt(minute);
    }

    private long getDateTime(String datetime) throws ParseException {
        return (dateFmt.parse(datetime).getTime() + timeOffset) / 1000 / 60;
    }

    private long max(long a, long b) {
        return a > b ? a : b;
    }

    private long min(long a, long b) {
        return a > b ? b : a;
    }

    private void processLine(Hashtable<Integer, Long> map, String line) throws ParseException {
        Matcher m = timeSegment.matcher(line);
        if (m.matches() && m.groupCount() == 5) {
            String hour = m.group(1);
            String min = m.group(2);
            int start = getTimeVal(hour, min);
            int end = getTimeVal(m.group(3), m.group(4));
            while (start <= end) {
                map.put(start++, parseVal(m.group(5)));
            }
            return;
        }
        m = dateSegment.matcher(line);
        if (m.matches() && m.groupCount() == 3) {
            long now = (new Date().getTime() + timeOffset) / 1000 / 60;
            long max = now + 60 * 24;
            long start = max(getDateTime(m.group(1)), now);
            long end = min(getDateTime(m.group(2)), max);

            while (start < end) {
                int off = (int) (start % (24 * 60));
                map.put(off, parseVal(m.group(3)));
                start++;
            }
        }
    }


    private FileInputStream readFile() throws IOException {
        long length = file.length();
        if (length < cacheFileContent.length || cacheFileContent.length == 0) {
            return null;
        }
        FileInputStream fis = null;
        byte[] data = new byte[cacheFileContent.length];
        try {
            fis = new FileInputStream(file);
            long readBytes = fis.read(data);
            if (readBytes != cacheFileContent.length) {
                fis.close();
                return null;
            }
        } catch (IOException e) {
            if (fis != null) {
                fis.close();
            }
            throw e;
        }
        if (Arrays.equals(cacheFileContent, data)) {
            return fis;
        }
        fis.close();
        return null;
    }

    private void processAllFile() throws FileNotFoundException {
        Scanner s = new Scanner(file);
        Hashtable<Integer, Long> map = new Hashtable<Integer, Long>(60 * 24);
        while (s.hasNextLine()) {
            try {
                processLine(map, s.nextLine());
            } catch (ParseException e) {
                logger.error(e.getMessage());
            }
        }
        synchronized (lock) {
            baseMap = map;
        }
    }

    private void processAppendFile(FileInputStream r) {
        Scanner s = new Scanner(r);

        while (s.hasNextLine()) {
            try {
                synchronized (lock) {
                    processLine(baseMap, s.nextLine());
                }
            } catch (ParseException e) {
                logger.error(e.getMessage());
            }
        }
    }

    private void processFile(FileInputStream r) throws FileNotFoundException {
        if (r == null) {
            processAllFile();
            return;
        }
        processAppendFile(r);
    }

    long processBytes = 0;
    long processSec = 0;
    long processMin = 0;
    long threshold = 0;

    public boolean canProcess(long size) {
        if (!init.get()) {
            return true;
        }
        if (forceRefresh.get()) {
            processSec = 0;
            processMin = 0;
            forceRefresh.set(false);
        }
        Date date = new Date();
        long ts = date.getTime() / 1000;
        if (ts != processSec) {
            if (processBytes > threshold && threshold > 0) {
                logger.info("last second process: " + processBytes + ", threshold: " + threshold);
            }
            processSec = ts;
            processBytes = 0;
            long min = ts / 60;
            if (processMin != min) {
                processMin = min;
                synchronized (lock) {
                    Long val = baseMap.get(date.getHours() * 60 + date.getMinutes());
                    if (val == null) {
                        val = Long.valueOf(0);
                    }
                    threshold = val;
                }
            }
        }

        if (threshold > 0 && processBytes > threshold) {
            return false;
        }
        processBytes += size;
        return true;
    }
}
