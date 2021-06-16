package com.finsol.Model;

public class RedisHSetRecordModel {

    private byte[] key;
    private byte[] field;
    private byte[] value;


    public RedisHSetRecordModel(byte[] key, byte[] field, byte[] value) {
        this.key = key;
        this.field = field;
        this.value = value;
    }

    public RedisHSetRecordModel() {

    }


    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getField() {
        return field;
    }

    public void setField(byte[] field) {
        this.field = field;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
