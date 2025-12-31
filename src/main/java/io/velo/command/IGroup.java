package io.velo.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.Debug;
import io.velo.dyn.CachedGroovyClassLoader;
import io.velo.dyn.RefreshLoader;
import io.velo.ingest.ParquetGroupToValue;
import io.velo.reply.*;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;

public class IGroup extends BaseCommand {
    public IGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("incr".equals(cmd) || "incrby".equals(cmd) || "incrbyfloat".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("incr".equals(cmd)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }

            var dGroup = new DGroup(cmd, data, socket);
            dGroup.from(this);
            dGroup.setSlotWithKeyHashListParsed(this.slotWithKeyHashListParsed);

            return dGroup.decrBy(-1, 0);
        }

        if ("incrby".equals(cmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            int by;
            try {
                by = Integer.parseInt(new String(data[2]));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }

            var dGroup = new DGroup(cmd, data, socket);
            dGroup.from(this);
            dGroup.setSlotWithKeyHashListParsed(this.slotWithKeyHashListParsed);

            return dGroup.decrBy(-by, 0);
        }

        if ("incrbyfloat".equals(cmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            double by;
            try {
                by = Double.parseDouble(new String(data[2]));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_FLOAT;
            }

            var dGroup = new DGroup(cmd, data, socket);
            dGroup.from(this);
            dGroup.setSlotWithKeyHashListParsed(this.slotWithKeyHashListParsed);

            return dGroup.decrBy(0, -by);
        }

        if ("info".equals(cmd)) {
            return info();
        }

        if ("ingest".equals(cmd)) {
            return ingest();
        }

        if ("ingest_sst".equals(cmd)) {
            return ingest_sst();
        }

        return NilReply.INSTANCE;
    }

    private final CachedGroovyClassLoader cl = CachedGroovyClassLoader.getInstance();

    private Reply info() {
        var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/InfoCommandHandle.groovy");

        var variables = new HashMap<String, Object>();
        variables.put("iGroup", this);
        return (Reply) cl.eval(scriptText, variables);
    }

    private static String getPairValue(String pair) {
        var index = pair.indexOf("=");
        if (index == -1) {
            return pair;
        }

        return pair.substring(index + 1);
    }

    private Reply ingest() {
        // ingest dir=/tmp/velo/ingest/ file_pattern=test* file_format=json key_field=name key_prefix=test_ value_fields=*
        // ingest dir=/tmp/velo/ingest/ file_pattern=test* file_format=csv key_field_index=0 key_prefix=test_ value_fields_indexes=0,1,2,3
        // ingest dir=/tmp/velo/ingest/ file_pattern=test* file_format=parquet key_field=name key_prefix=test_ value_fields=* schema_file=test.schema
        if (data.length != 7 && data.length != 8) {
            return ErrorReply.FORMAT;
        }

        var dirPath = getPairValue(new String(data[1]));
        var filePattern = getPairValue(new String(data[2]));
        var fileFormat = getPairValue(new String(data[3]));
        var keyField = getPairValue(new String(data[4]));
        var keyPrefix = getPairValue(new String(data[5]));
        var valueFields = getPairValue(new String(data[6]));

        // only support csv, json or parquet
        var isCsv = "csv".equals(fileFormat);
        var isJson = "json".equals(fileFormat);

        int keyFieldIndex = 0;
        int[] valueFieldsIndexes;
        if (isCsv) {
            try {
                keyFieldIndex = Integer.parseInt(keyField);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }

            var array = valueFields.split(",");
            valueFieldsIndexes = new int[array.length];
            try {
                for (int i = 0; i < array.length; i++) {
                    valueFieldsIndexes[i] = Integer.parseInt(array[i]);
                }
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
        } else {
            valueFieldsIndexes = null;
        }

        String[] valueFieldsArray;
        if (isJson) {
            valueFieldsArray = valueFields.split(",");
        } else {
            valueFieldsArray = null;
        }

        var dir = new File(dirPath);
        if (!dir.exists()) {
            return new ErrorReply("dir: " + dirPath + " not exists");
        }

        File[] files = dir.listFiles();
        if (files == null) {
            return new ErrorReply("dir: " + dirPath + " is empty");
        }

        ArrayList<File> list = new ArrayList<>();
        for (var file : files) {
            if (file.getName().matches(filePattern)) {
                list.add(file);
            }
        }
        if (list.isEmpty()) {
            return new ErrorReply("dir: " + dirPath + " file_pattern: " + filePattern + " not match any file");
        }

        File schemaFile = null;
        MessageType schema = null;
        if (!isCsv && !isJson) {
            if (data.length != 8) {
                return ErrorReply.FORMAT;
            }

            schemaFile = new File(dir, getPairValue(new String(data[7])));
            if (!schemaFile.exists()) {
                return new ErrorReply("schema file: " + schemaFile.getAbsolutePath() + " not exists");
            }
            try {
                schema = MessageTypeParser.parseMessageType(Files.readString(schemaFile.toPath()));
            } catch (IOException e) {
                log.error("schema file={} read error", schemaFile.getAbsolutePath(), e);
                return new ErrorReply("schema file: " + schemaFile.getAbsolutePath() + " read error");
            }
        }

        var debug = Debug.getInstance();
        debug.bulkLoad = true;
        log.warn("Ingest start, set bulk load to true");

        int finalKeyFieldIndex = keyFieldIndex;
        MessageType finalSchema = schema;
        return localPersist.doSthInSlots(oneSlot -> {
            // put number + skip number
            int[] r = new int[2];

            for (var file : list) {
                if (isCsv || isJson) {
                    var objectMapper = new ObjectMapper();
                    try (var br = new BufferedReader(new FileReader(file))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            if (isCsv) {
                                var lineArray = line.split(",");
                                if (finalKeyFieldIndex >= lineArray.length) {
                                    throw new IllegalArgumentException("key_field_index: " + finalKeyFieldIndex + " >= line_array.length: " + lineArray.length);
                                }

                                var key = keyPrefix + lineArray[finalKeyFieldIndex];
                                var slotWithKeyHash = slot(key);
                                if (slotWithKeyHash.slot() != oneSlot.slot()) {
                                    r[1]++;
                                    continue;
                                }

                                r[0]++;

                                String value;
                                if (valueFieldsIndexes.length == lineArray.length) {
                                    value = line;
                                } else {
                                    var sb = new StringBuilder();
                                    for (int i = 0; i < valueFieldsIndexes.length; i++) {
                                        sb.append(lineArray[valueFieldsIndexes[i]]);
                                        if (i != valueFieldsIndexes.length - 1) {
                                            sb.append(",");
                                        }
                                    }
                                    value = sb.toString();
                                }
                                set(value.getBytes(), slotWithKeyHash);
                            } else {
                                // json
                                var map = objectMapper.readValue(line, HashMap.class);
                                if (!map.containsKey(keyField)) {
                                    throw new IllegalArgumentException("json file: " + file.getAbsolutePath() + " key_field: " + keyField + " not found");
                                }

                                var key = keyPrefix + map.get(keyField);
                                var slotWithKeyHash = slot(key);
                                if (slotWithKeyHash.slot() != oneSlot.slot()) {
                                    r[1]++;
                                    continue;
                                }

                                r[0]++;

                                String value;
                                if (valueFieldsArray.length == map.size()) {
                                    value = line;
                                } else {
                                    var subMap = new HashMap<>();
                                    for (int i = 0; i < valueFieldsArray.length; i++) {
                                        subMap.put(valueFieldsArray[i], map.get(valueFieldsArray[i]));
                                    }
                                    value = objectMapper.writeValueAsString(subMap);
                                }
                                set(value.getBytes(), slotWithKeyHash);
                            }
                        }
                    } catch (IOException e) {
                        log.error("read file error, file={}", file.getName(), e);
                    } finally {
                        oneSlot.resetWritePositionAfterBulkLoad();
                    }
                } else {
                    // parquet file
                    var objectMapper = new ObjectMapper();

                    var path = new Path(file.getAbsolutePath());
                    var readSupport = new GroupReadSupport();
                    ParquetReader.Builder<Group> readerBuilder = ParquetReader.builder(readSupport, path);
                    try (ParquetReader<Group> reader = readerBuilder.build()) {
                        Group group;
                        outer:
                        while ((group = reader.read()) != null) {
                            String key = null;
                            SlotWithKeyHash s = null;
                            HashMap<String, Object> map = new HashMap<>();
                            for (var field : finalSchema.getFields()) {
                                var fieldName = field.getName();
                                var fieldValue = ParquetGroupToValue.getFieldValue(group, fieldName, field);

                                if (fieldName.equals(keyField)) {
                                    key = keyPrefix + fieldValue;
                                    s = slot(key);
                                    if (s.slot() != oneSlot.slot()) {
                                        r[1]++;
                                        continue outer;
                                    }

                                    r[0]++;
                                }

                                map.put(fieldName, fieldValue);
                            }

                            if (key == null) {
                                throw new IllegalArgumentException("parquet file: " + file.getAbsolutePath() + " key_field: " + keyField + " not found");
                            }
                            set(objectMapper.writeValueAsBytes(map), s);
                        }
                    } catch (IOException e) {
                        log.error("read parquet file error, file={}", file.getName(), e);
                    } finally {
                        oneSlot.resetWritePositionAfterBulkLoad();
                    }
                }
            }

            return r;
        }, resultList -> {
            var replies = new Reply[resultList.size()];
            for (int i = 0; i < resultList.size(); i++) {
                var r = resultList.get(i);
                var str = "slot: " + i + " put: " + r[0] + " skip: " + r[1];
                replies[i] = new BulkReply(str);
            }

            debug.bulkLoad = false;
            log.warn("Ingest end, set bulk load to false");

            return new MultiBulkReply(replies);
        });
    }

    private Reply ingest_sst() {
        // ingest_sst dir=/tmp/velo/rocks_db_dir/
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var dirPath = getPairValue(new String(data[1]));
        var dir = new File(dirPath);
        if (!dir.exists()) {
            return new ErrorReply("dir: " + dirPath + " not exists");
        }

        File[] files = dir.listFiles();
        if (files == null) {
            return new ErrorReply("dir: " + dirPath + " is empty");
        }

        RocksDB db;
        try {
            db = RocksDB.open(new Options(), dirPath);
            log.info("open rocks db success, dir={}", dirPath);
        } catch (RocksDBException e) {
            log.error("open rocks db error, dir={}", dirPath, e);
            return new ErrorReply("open rocks db error, dir: " + dirPath);
        }

        // try to iterate first 10000 keys
        final int batchSize = 10000;
        log.info("try to do iterate, only iterate first {} keys.", batchSize);

        var iterator = db.newIterator();
        iterator.seekToFirst();
        int count = 0;
        while (iterator.isValid() && count < batchSize) {
            var keyBytes = iterator.key();
            var valueBytes = iterator.value();
            iterator.next();
            count++;

            if (count % 100 == 0) {
                log.info("key={}, value bytes size={}", new String(keyBytes), valueBytes.length);
            }
        }

        var debug = Debug.getInstance();
        debug.bulkLoad = true;
        log.warn("Ingest sst start, set bulk load to true");

        return localPersist.doSthInSlots(oneSlot -> {
            // put number + skip number
            int[] r = new int[2];
            try {
                var it = db.newIterator();
                it.seekToFirst();
                while (it.isValid()) {
                    var keyBytes = it.key();
                    var valueBytes = it.value();

                    var slotWithKeyHash = slot(keyBytes, slotNumber);
                    if (slotWithKeyHash.slot() != oneSlot.slot()) {
                        r[1]++;
                    } else {
                        set(valueBytes, slotWithKeyHash);
                        r[0]++;
                    }

                    it.next();
                }
            } finally {
                oneSlot.resetWritePositionAfterBulkLoad();
            }
            return r;
        }, resultList -> {
            db.close();
            log.info("close rocks db, dir={}", dirPath);

            var replies = new Reply[resultList.size()];
            for (int i = 0; i < resultList.size(); i++) {
                var r = resultList.get(i);
                var str = "slot: " + i + " put: " + r[0] + " skip: " + r[1];
                replies[i] = new BulkReply(str);
            }

            debug.bulkLoad = false;
            log.warn("Ingest sst end, set bulk load to false");

            return new MultiBulkReply(replies);
        });
    }
}
