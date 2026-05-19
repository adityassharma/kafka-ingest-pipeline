package io.github.adityassharma.kafka.pipeline;

import io.github.adityassharma.kafka.common.AppProperties;
import io.github.adityassharma.kafka.management.ManagementServer;
import io.github.adityassharma.kafka.spi.Sink;
import io.github.adityassharma.kafka.spi.Source;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;

/**
 * Unified pipeline entry point.
 *
 * <p>Usage:
 * <pre>
 *   java -cp kafka-ingest-pipeline-1.0.0-fat.jar \
 *     io.github.adityassharma.kafka.pipeline.PipelineMain \
 *     config/pipeline.properties
 * </pre>
 *
 * <p>Discovers all {@link Source} and {@link Sink} implementations via
 * {@link ServiceLoader}, then instantiates only those referenced in the
 * properties file.  Each source runs in its own {@link SourceRunner};
 * each sink runs in its own {@link SinkRunner}.
 */
public class PipelineMain {

    private static final Logger LOG = LogManager.getLogger(PipelineMain.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: PipelineMain <pipeline.properties>");
            System.exit(1);
        }

        AppProperties pipelineProps = AppProperties.load(args[0]);

        List<SourceRunner> sourceRunners = new ArrayList<>();
        List<SinkRunner>   sinkRunners   = new ArrayList<>();

        // ---- Sources (optional — absent or blank means this node runs no sources) ----
        String sourcesVal = pipelineProps.get("sources", "").trim();
        if (!sourcesVal.isEmpty()) {
            for (String rawName : sourcesVal.split(",")) {
                String sourceName = rawName.trim();
                if (sourceName.isEmpty()) continue;
                String prefix    = "source." + sourceName;
                Properties mergedProps  = extractAndMerge(pipelineProps, prefix);
                AppProperties sourceApp = AppProperties.fromProperties(mergedProps, prefix);

                String type   = mergedProps.getProperty("type");
                Source source = findSource(type);

                sourceRunners.add(new SourceRunner(sourceName, source, sourceApp, mergedProps));
            }
        }

        // ---- Sinks (optional — absent or blank means this node runs no sinks) ----
        String sinksVal = pipelineProps.get("sinks", "").trim();
        if (!sinksVal.isEmpty()) {
            for (String rawName : sinksVal.split(",")) {
                String sinkName = rawName.trim();
                if (sinkName.isEmpty()) continue;
                String prefix    = "sink." + sinkName;
                Properties mergedProps = extractAndMerge(pipelineProps, prefix);
                AppProperties sinkApp  = AppProperties.fromProperties(mergedProps, prefix);

                String type = mergedProps.getProperty("type");
                Sink   sink = findSink(type);

                sinkRunners.add(new SinkRunner(sinkName, sink, sinkApp, mergedProps));
            }
        }

        if (sourceRunners.isEmpty() && sinkRunners.isEmpty()) {
            LOG.warn("No sources or sinks configured - nothing to run. " +
                     "Set 'sources' and/or 'sinks' in the properties file.");
            return;
        }

        // ---- Management server (optional) ----
        ManagementServer mgmtServer = null;
        String mgmtPortStr = pipelineProps.get("management.port", null);
        if (mgmtPortStr != null) {
            int mgmtPort = Integer.parseInt(mgmtPortStr.trim());
            mgmtServer = new ManagementServer(
                mgmtPort,
                sourceRunners.stream().map(SourceRunner::getStats).toList(),
                sinkRunners.stream().map(SinkRunner::getStats).toList()
            );
            mgmtServer.start();
        }
        final ManagementServer finalMgmt = mgmtServer;

        // ---- Shutdown hook ----
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received - stopping pipeline");
            sourceRunners.forEach(SourceRunner::shutdown);
            sinkRunners.forEach(SinkRunner::shutdown);
            if (finalMgmt != null) finalMgmt.stop();
            LOG.info("Pipeline stopped");
        }, "pipeline-shutdown"));

        // ---- Start (sinks before sources to avoid lost records) ----
        sinkRunners.forEach(SinkRunner::start);
        sourceRunners.forEach(SourceRunner::start);

        LOG.info("Pipeline running: {} source(s), {} sink(s){}",
            sourceRunners.size(), sinkRunners.size(),
            mgmtServer != null ? " management=http://localhost:" + mgmtPortStr : "");
    }

    // -----------------------------------------------------------------------
    // SPI discovery
    // -----------------------------------------------------------------------

    private static Source findSource(String type) {
        if (type == null || type.isBlank())
            throw new IllegalArgumentException("source type is required");
        for (Source s : ServiceLoader.load(Source.class)) {
            if (s.type().equals(type)) return s;
        }
        throw new IllegalArgumentException(
            "No Source implementation found for type='" + type +
            "'. Register it in META-INF/services/io.github.adityassharma.kafka.spi.Source");
    }

    private static Sink findSink(String type) {
        if (type == null || type.isBlank())
            throw new IllegalArgumentException("sink type is required");
        for (Sink s : ServiceLoader.load(Sink.class)) {
            if (s.type().equals(type)) return s;
        }
        throw new IllegalArgumentException(
            "No Sink implementation found for type='" + type +
            "'. Register it in META-INF/services/io.github.adityassharma.kafka.spi.Sink");
    }

    // -----------------------------------------------------------------------
    // Config scoping: merge global properties with prefix-stripped instance props.
    // Instance-specific properties (prefix-stripped) override globals.
    // -----------------------------------------------------------------------

    static Properties extractAndMerge(AppProperties pipelineProps, String instancePrefix) {
        Properties raw    = pipelineProps.getRawProperties();
        String     prefix = instancePrefix + ".";
        Properties result = new Properties();

        // 1. Copy global properties (skip source./sink. prefixed keys and the
        //    top-level "sources"/"sinks" list entries).
        for (String key : raw.stringPropertyNames()) {
            if (!key.startsWith("source.") && !key.startsWith("sink.")
                    && !key.equals("sources") && !key.equals("sinks")) {
                result.setProperty(key, raw.getProperty(key));
            }
        }

        // 2. Copy instance-specific properties (prefix stripped), overriding globals.
        for (String key : raw.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                result.setProperty(key.substring(prefix.length()), raw.getProperty(key));
            }
        }

        return result;
    }
}
