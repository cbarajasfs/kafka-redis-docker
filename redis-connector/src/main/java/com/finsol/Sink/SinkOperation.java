package com.finsol.Sink;

import com.finsol.Model.RedisHSetRecordModel;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

abstract class SinkOperation {
    private static final Logger log = LoggerFactory.getLogger(SinkOperation.class);
    public static final SinkOperation NONE = new NoneOperation(null);

    public final Type type;
    private final MySinkConnectorConfig config;

    SinkOperation(Type type, MySinkConnectorConfig config) {
        this.type = type;
        this.config = config;
    }

    public enum Type {
        ADD,
        HSET,
        RPUSH,
        SET,
        DELETE,
        NONE
    }

    public abstract void add(byte[] key, byte[] value);
    public abstract void addHset(byte[] key, byte[] field, byte[] value);

    public abstract void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException;

    public abstract int size();

    protected void wait(RedisFuture<?> future) throws InterruptedException {
        log.info("wait() - future = {}", future);
        if (!future.await(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS)) {
            future.cancel(true);
            throw new RetriableException(
                    String.format("Timeout after %s ms while waiting for operation to complete.", this.config.operationTimeoutMs)
            );
        }
    }

    public static SinkOperation create(Type type, MySinkConnectorConfig config, int size) {
        SinkOperation result;

        switch (type) {
            case ADD:
                result = new AddOperation(config, size);
                break;
            case HSET:
                result = new HSetOperation(config, size);
                break;
            case RPUSH:
                result = new RPushOperation(config, size);
                break;
            case SET:
                result = new SetOperation(config, size);
                break;
            case DELETE:
                result = new DeleteOperation(config, size);
                break;
            default:
                throw new IllegalStateException(
                        String.format("%s is not a supported operation.", type)
                );
        }

        return result;
    }

    static class NoneOperation extends SinkOperation {
        NoneOperation(MySinkConnectorConfig config) {
            super(Type.NONE, config);
        }

        @Override
        public void add(byte[] key, byte[] value) {
            throw new UnsupportedOperationException(
                    "This should never be called."
            );
        }

        @Override
        public void addHset(byte[] key, byte[] field, byte[] value) {

        }

        @Override
        public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) {

        }

        @Override
        public int size() {
            return 0;
        }
    }

    static class AddOperation extends SinkOperation {
        final Map<byte[], byte[]> adds;

        AddOperation(MySinkConnectorConfig config, int size) {
            super(Type.ADD, config);
            this.adds = new LinkedHashMap<>(size);
        }

        @Override
        public void add(byte[] key, byte[] value) {
            this.adds.put(key, value);
        }

        @Override
        public void addHset(byte[] key, byte[] field, byte[] value) {

        }

        @Override
        public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
            log.info("execute() - Calling msetnx with {} value(s)", this.adds.size());
            RedisFuture<?> future = asyncCommands.msetnx(this.adds);
            wait(future);
        }

        @Override
        public int size() {
            return this.adds.size();
        }
    }

    static class RPushOperation extends SinkOperation {
        RedisHSetRecordModel item;

        RPushOperation(MySinkConnectorConfig config, int size) {
            super(Type.RPUSH, config);
            this.item = new RedisHSetRecordModel();
        }

        @Override
        public void add(byte[] key, byte[] value) {

        }

        @Override
        public void addHset(byte[] key, byte[] field, byte[] value) {
            this.item = new RedisHSetRecordModel(key, null, value);
        }

        @Override
        public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
            log.info("execute() - Calling HGET");
            RedisFuture<?> future = asyncCommands
                    .rpush(this.item.getKey(),this.item.getValue());
            wait(future);
        }

        @Override
        public int size() {
            return 1;
        }
    }

    static class HSetOperation extends SinkOperation {
        RedisHSetRecordModel hsets;

        HSetOperation(MySinkConnectorConfig config, int size) {
            super(Type.HSET, config);
            this.hsets = new RedisHSetRecordModel();
        }

        @Override
        public void add(byte[] key, byte[] value) {

        }

        @Override
        public void addHset(byte[] key, byte[] field, byte[] value) {
            this.hsets = new RedisHSetRecordModel(key, field, value);
        }


        @Override
        public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
            log.info("execute() - Calling HGET");
            RedisFuture<?> future = asyncCommands
                    .hset(this.hsets.getKey(), this.hsets.getField(), this.hsets.getValue());
            wait(future);
        }

        @Override
        public int size() {
            return 1;
        }
    }

    static class SetOperation extends SinkOperation {
        final Map<byte[], byte[]> sets;

        SetOperation(MySinkConnectorConfig config, int size) {
            super(Type.SET, config);
            this.sets = new LinkedHashMap<>(size);
        }

        @Override
        public void add(byte[] key, byte[] value) {
            this.sets.put(key, value);
        }

        @Override
        public void addHset(byte[] key, byte[] field, byte[] value) {

        }

        @Override
        public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
            log.info("execute() - Calling mset with {} value(s)", this.sets.size());
            RedisFuture<?> future = asyncCommands.mset(this.sets);
            wait(future);
        }

        @Override
        public int size() {
            return this.sets.size();
        }
    }

    static class DeleteOperation extends SinkOperation {
        final List<byte[]> deletes;

        DeleteOperation(MySinkConnectorConfig config, int size) {
            super(Type.DELETE, config);
            this.deletes = new ArrayList<>(size);
        }

        @Override
        public void add(byte[] key, byte[] value) {
            this.deletes.add(key);
        }

        @Override
        public void addHset(byte[] key, byte[] field, byte[] value) {

        }

        @Override
        public void execute(RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) throws InterruptedException {
            log.info("execute() - Calling del with {} value(s)", this.deletes.size());
            byte[][] deletes = this.deletes.toArray(new byte[this.deletes.size()][]);
            RedisFuture<?> future = asyncCommands.del(deletes);
            wait(future);
        }

        @Override
        public int size() {
            return this.deletes.size();
        }
    }
}
