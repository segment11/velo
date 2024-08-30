package io.velo.command;

public interface RDBCallback {
    void onInteger(Integer value);

    void onString(byte[] valueBytes);

    void onList(byte[] encodedBytes);

    void onSet(byte[] encodedBytes);

    void onZSet(byte[] encodedBytes);

    void onHashKeys(byte[] encodedBytes);

    void onHashFieldValues(String field, byte[] valueBytes);
}
