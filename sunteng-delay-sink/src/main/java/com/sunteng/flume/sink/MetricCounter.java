package com.sunteng.flume.sink;

import org.apache.flume.instrumentation.MonitoredCounterGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.util.ArrayList;

public class MetricCounter extends MonitoredCounterGroup implements
        DynamicMBean {
    private static final Logger logger = LoggerFactory
            .getLogger(MetricCounter.class);

    static final String[] hello = {};
    private ArrayList<Metric> metrics;

    private static class Metric {
        private String name;
        private long total;
        private long size;

        public Metric(String name, long x) {
            this.name = name;
            this.total = x;
            this.size = 1;
        }

        public long avg() {
            return total / size;
        }

        public void add(long s) {
            total += s;
            size += 1;
        }

        public String toString() {
            return this.total + "," + this.size;
        }
    }

    protected MetricCounter(String name) {
        super(MonitoredCounterGroup.Type.SINK, name, hello);
        metrics = new ArrayList<Metric>(512);
    }

    public void add(String name, long x) {
        Metric m;
        for (int i = 0; i < metrics.size(); i++) {
            m = metrics.get(i);
            if (m.name.equals(name)) {
                m.add(x);
                return;
            }

        }
        synchronized (metrics) {
            metrics.add(new Metric(name, x));
        }
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
        synchronized (metrics) {
            return new AttributeList(metrics.size()) {{
                for (Metric m : metrics) {
                    add(new Attribute(m.name, m.toString()));
                }
            }};
        }
    }

    @Override
    public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
        return null;
    }

    @Override
    public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
        return null;
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
        return null;
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        return new MBeanInfo("MetricCounter", "metric counter", null, null, null, null);
    }
}
