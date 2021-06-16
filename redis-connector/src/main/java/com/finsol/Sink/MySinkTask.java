package com.finsol.Sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.finsol.Redis.RedisSession;
import com.finsol.Redis.RedisSessionFactory;
import com.finsol.Redis.RedisSessionFactoryImpl;
import com.github.jcustenborder.kafka.connect.utils.data.SinkOffsetState;
import com.github.jcustenborder.kafka.connect.utils.data.TopicPartitionCounter;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;

public class MySinkTask extends SinkTask {
  /*
    Your connector should never use System.out for logging. All of your classes should use slf4j
    for logging
 */
  private static Logger log = LoggerFactory.getLogger(MySinkTask.class);

  private Gson gson = new Gson();

  MySinkConnectorConfig config;
  RedisSessionFactory sessionFactory = new RedisSessionFactoryImpl();
  RedisSession session;

  static SinkOffsetState state(KeyValue<byte[], byte[]> input) {
    if (!input.hasValue()) {
      return null;
    }
    try {
      return ObjectMapperFactory.INSTANCE.readValue(input.getValue(), SinkOffsetState.class);
    } catch (IOException e) {
      throw new DataException(e);
    }
  }

  @Override
  public void start(Map<String, String> settings) {
    this.config = new MySinkConnectorConfig(settings);
    this.session = this.sessionFactory.create(this.config);

    final Set<TopicPartition> assignment = this.context.assignment();
    if (!assignment.isEmpty()) {
      final byte[][] partitionKeys = assignment.stream()
              .map(MySinkTask::redisOffsetKey)
              .map(s -> s.getBytes(Charsets.UTF_8))
              .toArray(byte[][]::new);

      final RedisFuture<List<KeyValue<byte[], byte[]>>> partitionKeyFuture = this.session.asyncCommands().mget(partitionKeys);
      final List<SinkOffsetState> sinkOffsetStates;
      try {
        final List<KeyValue<byte[], byte[]>> partitionKey = partitionKeyFuture.get(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS);
        sinkOffsetStates = partitionKey.stream()
                .map(MySinkTask::state)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw new RetriableException(e);
      }
      Map<TopicPartition, Long> partitionOffsets = new HashMap<>(assignment.size());
      for (SinkOffsetState state : sinkOffsetStates) {
        partitionOffsets.put(state.topicPartition(), state.offset());
        log.info("Requesting offset {} for {}", state.offset(), state.topicPartition());
      }
      for (TopicPartition topicPartition : assignment) {
        if (!partitionOffsets.containsKey(topicPartition)) {
          partitionOffsets.put(topicPartition, 0L);
          log.info("Requesting offset {} for {}", 0L, topicPartition);
        }
      }
      this.context.offset(partitionOffsets);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {

    log.info("put() - Processing {} record(s)", records.size());
    List<SinkOperation> operations = new ArrayList<>(records.size());

    SinkOperation operation = SinkOperation.NONE;

    TopicPartitionCounter counter = new TopicPartitionCounter();

    for (SinkRecord record : records) {
      log.info("put() - Processing record " + formatLocation(record));
      log.info("put() - TOPIC NAME" + record.topic());

      byte[] key;
      if (null == record.key()) {
        if (null == record.topic() ) {
          throw new DataException(
                  "The key for the record cannot be null. " + formatLocation(record)
          );
        }

        key = toBytes("key", record.topic());
      } else {
        key = toBytes("key", record.key());
      }

      if (null == key || key.length == 0) {
        throw new DataException(
                "The key cannot be an empty byte array. " + formatLocation(record)
        );
      }
      final byte[] field = toBytes("field", String.valueOf(new Date().getTime()));
      final byte[] value = toBytes("value", record.value());

      SinkOperation.Type currentOperationType;

      if (null == value) {
        currentOperationType = SinkOperation.Type.DELETE;
      } else {
        currentOperationType = SinkOperation.Type.RPUSH;
      }

      if (currentOperationType != operation.type) {
        log.info(
                "put() - Creating new operation. current={} last={}",
                currentOperationType,
                operation.type
        );
        operation = SinkOperation.create(currentOperationType, this.config, records.size());
        operations.add(operation);
      }
      log.info("put() - Record Key {}", "rics");
      log.info("put() - Record field {}", record.key());

      operation.addHset(key, null, value);
      counter.increment(record.topic(), record.kafkaPartition(), record.kafkaOffset());
    }

    log.info(
            "put() - Found {} operation(s) in {} record{s}. Executing operations...",
            operations.size(),
            records.size()
    );

    final List<SinkOffsetState> offsetData = counter.offsetStates();

    if (!offsetData.isEmpty()) {
      operation = SinkOperation.create(SinkOperation.Type.SET, this.config, offsetData.size());
      operations.add(operation);
      for (SinkOffsetState e : offsetData) {
        final byte[] key = String.format("__kafka.offset.%s.%s", e.topic(), e.partition()).getBytes(Charsets.UTF_8);
        final byte[] value;
        try {
          value = ObjectMapperFactory.INSTANCE.writeValueAsBytes(e);
        } catch (JsonProcessingException e1) {
          throw new DataException(e1);
        }
        operation.add(key, value);
        log.info("put() - Setting offset: {}", e);
      }
    }

    for (SinkOperation op : operations) {
      log.info("put() - Executing {} operation with {} values", op.type, op.size());
      try {
        op.execute(this.session.asyncCommands());
      } catch (InterruptedException e) {
        throw new RetriableException(e);
      }
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

  }

  private static String redisOffsetKey(TopicPartition topicPartition) {
    return String.format("__kafka.offset.%s.%s", topicPartition.topic(), topicPartition.partition());
  }

  @Override
  public void stop() {
    try {
      if (null != this.session) {
        this.session.close();
      }
    } catch (Exception e) {
      log.warn("Exception thrown", e);
    }
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }


  private byte[] toBytes(String source, Object input) {
    final byte[] result;

    if (input instanceof String) {
      String s = (String) input;
      result = s.getBytes(this.config.charset);
    } else if (input instanceof byte[]) {
      result = (byte[]) input;
    } else if (null == input) {
      result = null;
    } else {
      throw new DataException(
              String.format(
                      "The %s for the record must be String or Bytes. Consider using the ByteArrayConverter " +
                              "or StringConverter if the data is stored in Kafka in the format needed in Redis. " +
                              "Another option is to use a single message transformation to transform the data before " +
                              "it is written to Redis.",
                      source
              )
      );
    }

    return result;
  }

  static String formatLocation(SinkRecord record) {
    return String.format(
            "topic = %s partition = %s offset = %s",
            record.topic(),
            record.kafkaPartition(),
            record.kafkaOffset()
    );
  }
}
