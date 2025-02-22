package io.velo.ingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to convert a Parquet Group object to various formats such as JSON and CSV.
 */
public class ParquetGroupToValue {
    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private ParquetGroupToValue() {
    }

    /**
     * Extracts the value of a field from a Parquet Group based on its type.
     *
     * @param group     the Parquet Group object containing the data
     * @param fieldName the name of the field whose value is to be retrieved
     * @param fieldType the type of the field
     * @return the value of the field converted to the corresponding Java type
     * @throws IllegalArgumentException if the field type is not supported
     */
    private static Object getFieldValue(Group group, String fieldName, Type fieldType) {
        PrimitiveType.PrimitiveTypeName primitiveTypeName = fieldType.asPrimitiveType().getPrimitiveTypeName();
        return switch (primitiveTypeName) {
            case INT32 -> group.getInteger(fieldName, 0);
            case INT64 -> group.getLong(fieldName, 0);
            case FLOAT -> group.getFloat(fieldName, 0);
            case DOUBLE -> group.getDouble(fieldName, 0);
            case BOOLEAN -> group.getBoolean(fieldName, 0);
            case BINARY -> group.getString(fieldName, 0);
            case FIXED_LEN_BYTE_ARRAY -> group.getBinary(fieldName, 0).getBytes();
            case INT96 -> group.getInt96(fieldName, 0);
            default -> throw new IllegalArgumentException("Unsupported type: " + primitiveTypeName);
        };
    }

    /**
     * Converts a Parquet Group to a JSON string.
     *
     * @param schema       the schema of the Parquet data
     * @param objectMapper the ObjectMapper to use for converting the data to JSON
     * @param group        the Parquet Group object containing the data
     * @return a JSON string representation of the Parquet Group
     * @throws JsonProcessingException if an error occurs during the conversion to JSON
     */
    public static String toJson(MessageType schema, ObjectMapper objectMapper, Group group) throws JsonProcessingException {
        Map<String, Object> record = new HashMap<>();
        for (Type field : schema.getFields()) {
            String fieldName = field.getName();
            Object fieldValue = getFieldValue(group, fieldName, field);
            record.put(fieldName, fieldValue);
        }

        return objectMapper.writeValueAsString(record);
    }

    /**
     * Converts a Parquet Group to a CSV string.
     *
     * @param schema the schema of the Parquet data
     * @param group  the Parquet Group object containing the data
     * @return a CSV string representation of the Parquet Group
     */
    public static String toCsv(MessageType schema, Group group) {
        StringBuilder sb = new StringBuilder();
        for (Type field : schema.getFields()) {
            String fieldName = field.getName();
            Object fieldValue = getFieldValue(group, fieldName, field);
            // For CSV, convert null to empty string to avoid issues with missing values
            sb.append(fieldValue != null ? fieldValue : "").append(",");
        }
        // Remove the trailing comma
        if (!sb.isEmpty()) {
            return sb.substring(0, sb.length() - 1);
        }
        return sb.toString();
    }
}