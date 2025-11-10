package com.microreactor;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.writer.DefaultRollingPolicy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;

public class ReactorJob {
    // Simple validator using dataset ranges from your README/proposal
    static boolean valid(JsonNode n) {
        // all keys should exist
        String[] keys = {"AT","V","AP","RH","PE","timestamp"};
        for (String k : keys) if (!n.has(k) || n.get(k).isNull()) return false;

        double AT = n.get("AT").asDouble();
        double V  = n.get("V").asDouble();
        double AP = n.get("AP").asDouble();
        double RH = n.get("RH").asDouble();
        double PE = n.get("PE").asDouble();

        // Ranges (from dataset description)
        boolean okAT = (AT >= 1.81  && AT <= 37.11);
        boolean okAP = (AP >= 992.89 && AP <= 1033.30);
        boolean okRH = (RH >= 25.56 && RH <= 100.16);
        boolean okV  = (V  >= 25.36 && V  <= 81.56);
        boolean okPE = (PE >= 420.0 && PE <= 496.0); // approx range from README

        return okAT && okAP && okRH && okV && okPE;
    }

    public static void main(String[] args) throws Exception {
        final String host = "127.0.0.1";
        final int port = 9999;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpoints (Exactly-once for FileSink on checkpoint)
        env.enableCheckpointing(5000); // every 5s
        // Optional tuning:
        // env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        // env.getCheckpointConfig().setCheckpointTimeout(30000);

        // 1) Socket source
        DataStream<String> raw = env.socketTextStream(host, port);

        // 2) Parse JSON & 3) Validate ranges
        ObjectMapper mapper = new ObjectMapper();
        DataStream<String> cleanedJson = raw
            .map(line -> {
                try {
                    JsonNode n = mapper.readTree(line);
                    return valid(n) ? mapper.writeValueAsString(n) : null;
                } catch (Exception e) {
                    return null; // drop malformed
                }
            })
            .filter(s -> s != null)
            // (Optional) assign watermarks if you later do time windows; not required for sink
            .assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(2)));

        // 4) Sink to newline-delimited JSON files (local filesystem)
        FileSink<String> sink = FileSink
            .forRowFormat(new Path("output/reactor_json"),
                new SimpleStringEncoder<String>("UTF-8"))
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(Duration.ofMinutes(5))
                    .withInactivityInterval(Duration.ofMinutes(1))
                    .withMaxPartSize(64 * 1024 * 1024)
                    .build()
            )
            .build();

        cleanedJson.sinkTo(sink);

        env.execute("Micro-Reactor Telemetry Ingestion");
    }
}
