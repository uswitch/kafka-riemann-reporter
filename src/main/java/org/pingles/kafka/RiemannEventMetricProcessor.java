package org.pingles.kafka;

import com.aphyr.riemann.Proto.Attribute;
import com.aphyr.riemann.Proto.Event;
import com.yammer.metrics.core.*;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class RiemannEventMetricProcessor implements MetricProcessor<Metric> {
    private static final Logger LOGGER = Logger.getLogger(RiemannReporter.class);
    private final LinkedList events;
    private final Event prototype;

    public RiemannEventMetricProcessor(Event prototype) {
        this.events = new LinkedList();
        this.prototype = prototype;
    }

    public List<Event> getEvents() {
        return Collections.unmodifiableList(events);
    }

    private void addEvent(Event event) {
        events.add(event);
    }

    @Override
    public void processMeter(MetricName name, Metered meter, Metric context) throws Exception {
        addEvent(buildEvent(String.format("%s mean", name.getName()))
                .setMetricD(meter.meanRate())
                .addAttributes(buildMetricTypeAttribute("meter"))
                .build());
        addEvent(buildEvent(String.format("%s count", name.getName()))
                .setMetricD(meter.count())
                .addAttributes(buildMetricTypeAttribute("meter"))
                .build());
        addEvent(buildEvent(String.format("%s oneMinute", name.getName()))
                .setMetricD(meter.oneMinuteRate())
                .addAttributes(buildMetricTypeAttribute("meter"))
                .build());
        addEvent(buildEvent(String.format("%s fiveMinute", name.getName()))
                .setMetricD(meter.fiveMinuteRate())
                .addAttributes(buildMetricTypeAttribute("meter"))
                .build());
        addEvent(buildEvent(String.format("%s fifteenMinute", name.getName()))
                .setMetricD(meter.fifteenMinuteRate())
                .addAttributes(buildMetricTypeAttribute("meter"))
                .build());
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Metric context) throws Exception {
        addEvent(buildEvent(name.getName())
                .setMetricSint64(counter.count())
                .addAttributes(buildMetricTypeAttribute("counter"))
                .build());
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Metric context) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("Ignoring histogram: %s", name.getName()));
        }
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Metric context) throws Exception {
        addEvent(buildEvent(String.format("%s mean", name.getName()))
                .setMetricD(timer.mean())
                .addAttributes(buildMetricTypeAttribute("timer"))
                .build());
        addEvent(buildEvent(String.format("%s one minute", name.getName()))
                .setMetricD(timer.oneMinuteRate())
                .addAttributes(buildMetricTypeAttribute("timer"))
                .build());
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Metric context) throws Exception {
        Event.Builder builder = buildEvent(name.getName())
                .addAttributes(buildMetricTypeAttribute("gauge"));

        Object value = gauge.value();
        if (value instanceof Double) {
            builder.setMetricD((Double) value);
        } else if (value instanceof Integer) {
            builder.setMetricSint64(((Integer) value).longValue());
        } else if (value instanceof Long) {
            builder.setMetricSint64((Long)value);
        } else {
            builder.setState(value.toString());
        }

        addEvent(builder.build());
    }

    private Event.Builder buildEvent(String serviceLabel) {
        return Event.newBuilder(prototype).setService(serviceLabel);
    }

    private Attribute buildMetricTypeAttribute(String type) {
        return Attribute.newBuilder().setKey("metricType").setValue(type).build();
    }
}
