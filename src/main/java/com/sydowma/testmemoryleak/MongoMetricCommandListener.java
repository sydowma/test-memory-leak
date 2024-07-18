package com.sydowma.testmemoryleak;

import com.mongodb.event.CommandListener;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.mongodb.MongoMetricsCommandListener;
import lombok.extern.slf4j.Slf4j;

import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MongoMetricCommandListener implements CommandListener {
    static final Set<String> COMMANDS_WITH_COLLECTION_NAME = new LinkedHashSet<>(Arrays.asList(
            "aggregate", "count", "distinct", "mapReduce", "geoSearch", "delete", "find", "findAndModify",
            "insert", "update", "collMod", "compact", "convertToCapped", "create", "createIndexes", "drop",
            "dropIndexes", "killCursors", "listIndexes", "reIndex"));

    private MeterRegistry meterRegistry;

    private static final int SLOW_QUERY_TIME = 300;
    public static final String ADMIN = "admin";

    private final Counter commandSuccessCounter;
    private final Counter commandErrorCounter;
    private final Map<String, Timer> timerCache = new ConcurrentHashMap<>();
    private final Map<String, Counter> errorCounterCache = new ConcurrentHashMap<>();
    private final Map<String, Counter> slowQueryCounterCache = new ConcurrentHashMap<>();
    private final Map<String, Counter> commandFailedCounterCache = new ConcurrentHashMap<>();

    private final Map<Integer, String> context = new ConcurrentHashMap<>();

    private final AtomicLong oneMinuteCounter = new AtomicLong();
    private final AtomicLong fiveMinuteCounter = new AtomicLong();
    private final AtomicLong fifteenMinuteCounter = new AtomicLong();

    public MongoMetricCommandListener(MeterRegistry meterRegistry) {
        Meter.Id meterId = new Meter.Id("mongodb.default.meter",
                                        Tags.of(List.of(Tag.of("type", "custom"))), null, null, Meter.Type.GAUGE);
//        defaultMeter = new DefaultMeter(meterId, 1.0, 5.0, 15.0);
        this.meterRegistry = meterRegistry;
        this.commandSuccessCounter = meterRegistry.counter("mongodb.command.success");
        this.commandErrorCounter = meterRegistry.counter("mongodb.command.error");
    }

    private Timer getOrCreateTimer(String commandName, String collection, String clusterId) {
        String key = commandName + ":" + collection + ":" + clusterId;
        return timerCache.computeIfAbsent(key, k -> Timer.builder("mongodb.command.timer")
                    .tag("commandName", commandName)
                    .tag("collection", collection)
                    .tag("mongodb.cluster_id", clusterId)
                    .register(meterRegistry));
    }

    private Counter getOrCreateErrorCounter(String commandName, String collection, String clusterId, Map<String, String> msg) {
        String key = commandName + ":" + collection + ":" + clusterId + ":" + msg;
        Counter.Builder tag = Counter.builder("mongodb.command.error.count")
            .tag("commandName", commandName)
            .tag("collection", collection)
            .tag("mongodb.cluster_id", clusterId);
        for (Map.Entry<String, String> entry : msg.entrySet()) {
            tag.tag("code", entry.getValue());
            tag.tag("key", entry.getKey());
        }
        Counter register = tag.register(meterRegistry);
        return errorCounterCache.computeIfAbsent(key, k -> register);
    }

    private Counter getOrCreateSlowQueryCounter(String commandName, String collection, String clusterId) {
        String key = commandName + ":" + collection + ":" + clusterId;
        return slowQueryCounterCache.computeIfAbsent(key, k -> Counter.builder("mongodb.command.slowQuery.count")
                    .tag("commandName", commandName)
                    .tag("collection", collection)
                    .tag("mongodb.cluster_id", clusterId)
                    .register(meterRegistry));
    }

    private Counter getOrCreateCommandFailedCounter(String commandName, String collection, String clusterId, String msg) {
        String key = commandName + ":" + collection + ":" + clusterId + ":" + msg;
        return commandFailedCounterCache.computeIfAbsent(key, k -> Counter.builder("mongodb.command.failed.count")
                    .tag("commandName", commandName)
                    .tag("collection", collection)
                    .tag("mongodb.cluster_id", clusterId)
                    .tag("msg", msg)
                    .register(meterRegistry));
    }

    @Override
        public void commandStarted(CommandStartedEvent event) {
        // Implementation not required for this example
        String collection = getCollectionName(event.getCommand(), event.getCommandName());
        context.put(event.getRequestId(), collection);
    }

    @Override
        public void commandSucceeded(CommandSucceededEvent event) {
        long duration = event.getElapsedTime(TimeUnit.MILLISECONDS);

        BsonDocument bsonDocument = event.getResponse();
        BsonValue writeErrors = bsonDocument.get("writeErrors");
        boolean success = writeErrors == null;

        if (!COMMANDS_WITH_COLLECTION_NAME.contains(event.getCommandName())) {
            return;
        }

        if (ADMIN.equals(event.getDatabaseName())) {
            return;
        }

        Map<String, String> msg = findMsg(success, writeErrors);

        String collection = Optional.ofNullable(context.get(event.getRequestId())).orElse("");

        this.collectMetric(event.getCommandName(), collection, duration, success, event.getConnectionDescription(), msg);
    }

    public static String getCollectionName(BsonDocument command, String commandName) {
        if (COMMANDS_WITH_COLLECTION_NAME.contains(commandName)) {
            String collectionName = getNonEmptyBsonString(command.get(commandName));
            if (collectionName != null) {
                return collectionName;
            }
        }
        // Some other commands, like getMore, have a field like {"collection": collectionName}.
        return getNonEmptyBsonString(command.get("collection"));
    }

    /**
    * @return trimmed string from {@code bsonValue} or null if the trimmed string was empty or the
    * value wasn't a string
    */
    protected static String getNonEmptyBsonString(BsonValue bsonValue) {
        if (bsonValue == null || !bsonValue.isString()) return null;
        String stringValue = bsonValue.asString().getValue().trim();
        return stringValue.isEmpty() ? null : stringValue;
    }


    private static Map<String, String> findMsg(boolean success, BsonValue writeErrors) {
        Map<String, String> msg = new HashMap<>();
        if (!success) {
            if (writeErrors.isArray()) {
                for (BsonValue bsonValue : writeErrors.asArray()) {
                    if (bsonValue.isDocument()) {
                        BsonDocument document = bsonValue.asDocument();
                        BsonValue errorCode = document.get("code");
                        BsonValue keyPattern = document.get("keyPattern");
                        int code = errorCode.asInt32().getValue();
                        String key = keyPattern.toString();
                        msg.put(key, String.valueOf(code));
                        break;
                    }
                }
            }
        }
        return msg;
    }

    private void collectMetric(String commandName, String collection, long duration, boolean success,
                               ConnectionDescription connectionDescription, Map<String, String> msg) {
        String clusterId = connectionDescription.getConnectionId().getServerId().getClusterId().getValue();
        Timer commandSpecificTimer = this.getOrCreateTimer(commandName, collection, clusterId);
        commandSpecificTimer.record(duration, TimeUnit.MILLISECONDS);

        if (success) {
            commandSuccessCounter.increment();
        } else {
            commandErrorCounter.increment();
        }

        if (!msg.isEmpty()) {
            Counter errorCounter = getOrCreateErrorCounter(commandName, collection, clusterId, msg);
            errorCounter.increment();
        }

        // slow query
        if (duration > SLOW_QUERY_TIME) {
            Timer slowQueryTimer = Timer.builder("mongodb.command.slowQuery.timer")
                        .tag("commandName", commandName)
                        .tag("collection", collection)
                        .tag("mongodb.cluster_id", clusterId)
                        .register(meterRegistry);
            slowQueryTimer.record(duration, TimeUnit.MILLISECONDS);

            Counter slowQueryCounter = getOrCreateSlowQueryCounter(commandName, collection, clusterId);
            slowQueryCounter.increment();
        }

        updateRates();
    }

    @Override
        public void commandFailed(CommandFailedEvent commandFailedEvent) {
        commandErrorCounter.increment();

        String commandName = commandFailedEvent.getCommandName();
        ConnectionId connectionId = commandFailedEvent.getConnectionDescription().getConnectionId();
        String clusterId = connectionId.getServerId().getClusterId().getValue();
        String msg = commandFailedEvent.getThrowable().getMessage();

        String collection = Optional.ofNullable(context.get(commandFailedEvent.getRequestId())).orElse("");

        Counter commandFailedCounter = getOrCreateCommandFailedCounter(commandName, collection, clusterId, msg);
        commandFailedCounter.increment();
    }

    private void updateRates() {
        oneMinuteCounter.incrementAndGet();
        fiveMinuteCounter.incrementAndGet();
        fifteenMinuteCounter.incrementAndGet();


        // Reset the counters if needed to avoid overflow
        if (oneMinuteCounter.get() >= 60) {
            oneMinuteCounter.set(0);
        }
        if (fiveMinuteCounter.get() >= 300) {
            fiveMinuteCounter.set(0);
        }
        if (fifteenMinuteCounter.get() >= 900) {
            fifteenMinuteCounter.set(0);
        }
    }


}