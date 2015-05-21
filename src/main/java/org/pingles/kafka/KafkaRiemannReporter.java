package org.pingles.kafka;

import com.aphyr.riemann.client.RiemannClient;
import com.yammer.metrics.core.Clock;
import kafka.metrics.KafkaMetricsConfig;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

public class KafkaRiemannReporter implements KafkaRiemannReporterMBean, KafkaMetricsReporter {
    private static final Logger LOGGER = Logger.getLogger(KafkaRiemannReporter.class);
    private boolean initialized = false;
    private final Object lock = new Object();
    private RiemannClient riemannClient;
    private RiemannReporter reporter;

    public KafkaRiemannReporter() {
    }

    @Override
    public void init(VerifiableProperties props) {
        synchronized (lock) {
            if (!initialized && isEnabled(props)) {
                initialize(props);
                initialized = true;
            }
        }
    }

    private void initialize(VerifiableProperties props) {
        KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(props);
        try {
            riemannClient = riemannClientFromProperties(props);
            String reportedHostname = InetAddress.getLocalHost().getHostName();
            Clock clock = Clock.defaultClock();
            reporter = new RiemannReporter(riemannClient, reportedHostname, clock);
            startReporter(metricsConfig.pollingIntervalSecs());
        } catch (Exception e) {
            throw new RuntimeException(String.format("Error initializing Riemann reporter"), e);
        }
    }

    private boolean isEnabled(VerifiableProperties props) {
        return props.getBoolean("kafka.riemann.metrics.reporter.enabled", false);
    }

    public static RiemannClient riemannClientFromProperties(VerifiableProperties props) throws IOException {
        String host = props.getString("kafka.riemann.metrics.reporter.publisher.host", "127.0.0.1");
        Integer port = props.getInt("kafka.riemann.metrics.reporter.publisher.port", 5555);

        RiemannClient riemannClient = RiemannClient.tcp(host, port);
        riemannClient.connect();
        return riemannClient;
    }

    @Override
    public void startReporter(long pollingPeriodInSeconds) {
        LOGGER.info(String.format("Starting Riemann metrics reporter, polling every %d seconds", pollingPeriodInSeconds));
        reporter.start(pollingPeriodInSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void stopReporter() {
        LOGGER.info(String.format("Stopping Riemann metrics reporter"));
        reporter.shutdown();
        riemannClient.close();
    }

    @Override
    public String getMBeanName() {
        return "kafka:type=org.pingles.kafka.KafkaRiemannReporter";
    }
}