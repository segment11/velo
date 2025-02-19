package io.velo.ingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.HashMap;
import java.util.Map;

public class ParquetGroupToValue {
    private ParquetGroupToValue(){}

    private static Object getFieldValue(Group group, String fieldName, Type fieldType) {
        return switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
            case INT32 -> group.getInteger(fieldName, 0);
            case INT64 -> group.getLong(fieldName, 0);
            case FLOAT -> group.getFloat(fieldName, 0);
            case DOUBLE -> group.getDouble(fieldName, 0);
            case BOOLEAN -> group.getBoolean(fieldName, 0);
            case BINARY -> group.getString(fieldName, 0);
            case FIXED_LEN_BYTE_ARRAY -> group.getBinary(fieldName, 0).getBytes();
            case INT96 -> group.getInt96(fieldName, 0);
            default ->
                    throw new IllegalArgumentException("Unsupported type: " + fieldType.asPrimitiveType().getPrimitiveTypeName());
        };
    }

    public static String toJson(MessageType schema, ObjectMapper objectMapper, Group group) throws JsonProcessingException {
        Map<String, Object> record = new HashMap<>();
        for (Type field : schema.getFields()) {
            var fieldName = field.getName();
            var fieldValue = getFieldValue(group, fieldName, field);
            record.put(fieldName, fieldValue);
        }

        return objectMapper.writeValueAsString(record);
    }

    public static String toCsv(MessageType schema, Group group) {
        var sb = new StringBuilder();
        for (Type field : schema.getFields()) {
            var fieldName = field.getName();
            var fieldValue = getFieldValue(group, fieldName, field);
            sb.append(fieldValue).append(",");
        }
        // remove last ,
        return sb.substring(0, sb.length() - 1);
    }
}
