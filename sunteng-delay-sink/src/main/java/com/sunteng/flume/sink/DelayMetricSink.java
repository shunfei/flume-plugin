package com.sunteng.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelayMetricSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory
            .getLogger(DelayMetricSink.class);

    private static final String CONF_DEFAULT_PREFIX = "tsgen";
    private static final String CONF_PREFIX = "prefix";
    private static String prefix;

    private MetricCounter sinkCounter;

    public DelayMetricSink() {
    }

    @Override
    public void configure(Context context) {
        prefix = context.getString(CONF_PREFIX, CONF_DEFAULT_PREFIX);
        if (sinkCounter == null) {
            sinkCounter = new MetricCounter(getName());
        }
    }

    @Override
    public void start() {
        logger.info("Starting {}...", this);
        sinkCounter.start();
        super.start();

        logger.info("DelayMetricSink {} started.", getName());
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;

        try {
            transaction.begin();
            event = channel.take();
            if (event != null) {
                String body = new String(event.getBody());
                String[] sp = body.split(" ");
                if (sp == null || !sp[0].equals(prefix)) {
                    logger.error("invalid delayMetric body: " + body);
                } else {
                    String name = sp[1];
                    long ts = new java.util.Date().getTime()/1000;
                    long tss = Long.parseLong(sp[2]);
                    logger.info(ts + "," + tss);
                    if (ts >= tss && tss >0) {
                        sinkCounter.add(name, ts-tss);
                    }
                }
            }
            transaction.commit();
        } catch (Exception ex) {
            transaction.rollback();
            throw new EventDeliveryException("Failed to process transaction", ex);
        } finally {
            transaction.close();
        }

        return Status.READY;
    }

    @Override
    public void stop() {
        logger.info("RollingFile sink {} stopping...", getName());
        sinkCounter.stop();
        super.stop();
        logger.info("RollingFile sink {} stopped. Event metrics: {}",
                getName(), sinkCounter);
    }


}
