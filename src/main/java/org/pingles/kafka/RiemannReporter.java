package org.pingles.kafka;

import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.Proto.Msg;
import com.aphyr.riemann.client.IPromise;
import com.aphyr.riemann.client.RiemannClient;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RiemannReporter extends AbstractPollingReporter  {
    private static final Logger LOGGER = Logger.getLogger(RiemannReporter.class);
    private static final long RIEMANN_TIMEOUT_MS = 5000;
    private final RiemannClient riemannClient;
    private final String reportedHostname;
    private final Clock clock;

    public RiemannReporter(RiemannClient riemannClient, String reportedHostname, Clock clock) {
        super(Metrics.defaultRegistry(), "riemann-reporter");
        this.riemannClient = riemannClient;
        this.reportedHostname = reportedHostname;
        this.clock = clock;
    }

    @Override
    public void run() {
        final Event prototype = buildPrototypeEvent();
        final RiemannEventMetricProcessor metricProcessor = new RiemannEventMetricProcessor(prototype);

        final Set<Map.Entry<MetricName, Metric>> metrics = getMetricsRegistry().allMetrics().entrySet();
        for (Map.Entry<MetricName, Metric> metricEntry : metrics) {
            MetricName name = metricEntry.getKey();
            Metric metric = metricEntry.getValue();

            try {
                metric.processWith(metricProcessor, name, metric);
            } catch (Exception e) {
                LOGGER.error("Couldn't process metric", e);
            }
        }

        try {
            IPromise<Msg> promise = this.riemannClient.sendEvents(metricProcessor.getEvents());
            Msg res = promise.deref(RIEMANN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (res == null) {
                LOGGER.warn(String.format("Failed to send events to Riemann within %s ms", RIEMANN_TIMEOUT_MS));
            }
        } catch (IOException e) {
            LOGGER.error("Error sending events ");
        }
    }

    private Event buildPrototypeEvent() {
        return Event.newBuilder()
                .setHost(reportedHostname)
                .setTime(TimeUnit.MILLISECONDS.toSeconds(clock.time()))
                .addTags("kafkabroker")
                .build();
    }

}
