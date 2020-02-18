package jheister;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.threeten.bp.LocalDateTime;
import org.threeten.bp.ZoneId;
import org.threeten.bp.ZonedDateTime;
import org.threeten.bp.format.DateTimeFormatter;
import org.threeten.bp.temporal.TemporalAccessor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class JsonStreamToOrcConverter {
    private final TypeDescription schema;
    private final JsonConverter[] converters;
    private final Writer writer;
    private final VectorizedRowBatch batch;
    private final List<String> fieldNames;
    private final DateTimeFormatter dateTimeFormatter;


    public static void main(String[] args) throws IOException {
        JsonStreamToOrcConverter writer = new JsonStreamToOrcConverter("struct<price_date:string,id:string,close_price:double>", "out.orc", "");

        writer.write("{ \"price_date\": \"2020-01-01\", \"id\": \"1234\", \"close_price\": 123.45 }");
        writer.write("{ \"price_date\": \"2020-01-01\", \"id\": \"1234\", \"close_price\": 123.45 }");
        writer.write("{ \"price_date\": \"2020-01-01\", \"id\": \"1234\", \"close_price\": 123.45 }");

        writer.close();
    }




    public JsonStreamToOrcConverter(String schema, String outputFile, String timestampFormat) throws IOException {
        this.schema = TypeDescription.fromString(schema);
        fieldNames = this.schema.getFieldNames();

        List<TypeDescription> fieldTypes = this.schema.getChildren();

        this.converters = new JsonConverter[fieldTypes.size()];
        for(int c = 0; c < converters.length; ++c) {
            converters[c] = createConverter(fieldTypes.get(c));
        }

        writer = OrcFile.createWriter(new Path(outputFile), OrcFile.writerOptions(new Configuration()).setSchema(this.schema));
        batch = this.schema.createRowBatch();
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(timestampFormat);
    }


    public void write(String json) throws IOException {
        JsonObject elem = (JsonObject) new JsonParser().parse(json);
        for(int c=0; c < converters.length; ++c) {
            JsonElement field = elem.get(fieldNames.get(c));
            if (field == null) {
                batch.cols[c].noNulls = false;
                batch.cols[c].isNull[batch.size] = true;
            } else {
                converters[c].convert(field, batch.cols[c], batch.size);
            }
        }
        batch.size++;


        if (batch.getMaxSize() == batch.size) {
            writer.addRowBatch(batch);
            batch.reset();
        }
    }

    public void close() throws IOException {
        if (batch.size > 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();
    }

    JsonConverter createConverter(TypeDescription schema) {
        switch (schema.getCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return new LongColumnConverter();
            case FLOAT:
            case DOUBLE:
                return new DoubleColumnConverter();
            case CHAR:
            case VARCHAR:
            case STRING:
                return new StringColumnConverter();
            case DECIMAL:
                return new DecimalColumnConverter();
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
                return new TimestampColumnConverter();
            case BINARY:
                return new BinaryColumnConverter();
            case BOOLEAN:
                return new BooleanColumnConverter();
            case STRUCT:
                return new StructColumnConverter(schema);
            case LIST:
                return new ListColumnConverter(schema);
            case MAP:
                return new MapColumnConverter(schema);
            default:
                throw new IllegalArgumentException("Unhandled type " + schema);
        }
    }

    interface JsonConverter {
        void convert(JsonElement value, ColumnVector vect, int row);
    }

    static class BooleanColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                LongColumnVector vector = (LongColumnVector) vect;
                vector.vector[row] = value.getAsBoolean() ? 1 : 0;
            }
        }
    }

    static class LongColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                LongColumnVector vector = (LongColumnVector) vect;
                vector.vector[row] = value.getAsLong();
            }
        }
    }

    static class DoubleColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                DoubleColumnVector vector = (DoubleColumnVector) vect;
                vector.vector[row] = value.getAsDouble();
            }
        }
    }

    static class StringColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                BytesColumnVector vector = (BytesColumnVector) vect;
                byte[] bytes = value.getAsString().getBytes(StandardCharsets.UTF_8);
                vector.setRef(row, bytes, 0, bytes.length);
            }
        }
    }

    static class BinaryColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                BytesColumnVector vector = (BytesColumnVector) vect;
                String binStr = value.getAsString();
                byte[] bytes = new byte[binStr.length()/2];
                for(int i=0; i < bytes.length; ++i) {
                    bytes[i] = (byte) Integer.parseInt(binStr.substring(i*2, i*2+2), 16);
                }
                vector.setRef(row, bytes, 0, bytes.length);
            }
        }
    }


    class TimestampColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                TimestampColumnVector vector = (TimestampColumnVector) vect;
                TemporalAccessor temporalAccessor = dateTimeFormatter.parseBest(value.getAsString(),
                        ZonedDateTime.FROM, LocalDateTime.FROM);
                if (temporalAccessor instanceof ZonedDateTime) {
                    ZonedDateTime zonedDateTime = ((ZonedDateTime) temporalAccessor);
                    Timestamp timestamp = new Timestamp(zonedDateTime.toEpochSecond() * 1000L);
                    timestamp.setNanos(zonedDateTime.getNano());
                    vector.set(row, timestamp);
                } else if (temporalAccessor instanceof LocalDateTime) {
                    ZonedDateTime tz = ((LocalDateTime) temporalAccessor).atZone(ZoneId.systemDefault());
                    Timestamp timestamp = new Timestamp(tz.toEpochSecond() * 1000L);
                    timestamp.setNanos(tz.getNano());
                    vector.set(row, timestamp);
                } else {
                    vect.noNulls = false;
                    vect.isNull[row] = true;
                }
            }
        }
    }

    static class DecimalColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                DecimalColumnVector vector = (DecimalColumnVector) vect;
                vector.vector[row].set(HiveDecimal.create(value.getAsString()));
            }
        }
    }

    class StructColumnConverter implements JsonConverter {
        private JsonConverter[] childrenConverters;
        private List<String> fieldNames;

        public StructColumnConverter(TypeDescription schema) {
            List<TypeDescription> kids = schema.getChildren();
            childrenConverters = new JsonConverter[kids.size()];
            for(int c=0; c < childrenConverters.length; ++c) {
                childrenConverters[c] = createConverter(kids.get(c));
            }
            fieldNames = schema.getFieldNames();
        }

        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                StructColumnVector vector = (StructColumnVector) vect;
                JsonObject obj = value.getAsJsonObject();
                for(int c=0; c < childrenConverters.length; ++c) {
                    JsonElement elem = obj.get(fieldNames.get(c));
                    childrenConverters[c].convert(elem, vector.fields[c], row);
                }
            }
        }
    }

    class ListColumnConverter implements JsonConverter {
        private JsonConverter childrenConverter;

        public ListColumnConverter(TypeDescription schema) {
            childrenConverter = createConverter(schema.getChildren().get(0));
        }

        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                ListColumnVector vector = (ListColumnVector) vect;
                JsonArray obj = value.getAsJsonArray();
                vector.lengths[row] = obj.size();
                vector.offsets[row] = vector.childCount;
                vector.childCount += vector.lengths[row];
                vector.child.ensureSize(vector.childCount, true);
                for(int c=0; c < obj.size(); ++c) {
                    childrenConverter.convert(obj.get(c), vector.child,
                            (int) vector.offsets[row] + c);
                }
            }
        }
    }

    class MapColumnConverter implements JsonConverter {
        private JsonConverter keyConverter;
        private JsonConverter valueConverter;

        public MapColumnConverter(TypeDescription schema) {
            TypeDescription keyType = schema.getChildren().get(0);
            if (keyType.getCategory() != TypeDescription.Category.STRING)
                throw new IllegalArgumentException("JSON can only support MAP key in STRING type: " + schema);
            keyConverter = createConverter(keyType);
            valueConverter = createConverter(schema.getChildren().get(1));
        }

        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                MapColumnVector vector = (MapColumnVector) vect;
                JsonObject obj = value.getAsJsonObject();
                vector.lengths[row] = obj.entrySet().size();
                vector.offsets[row] = vector.childCount;
                vector.childCount += vector.lengths[row];
                vector.keys.ensureSize(vector.childCount, true);
                vector.values.ensureSize(vector.childCount, true);
                int cnt = 0;
                for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
                    int offset = (int) vector.offsets[row] + cnt++;
                    keyConverter.convert(new JsonPrimitive(entry.getKey()), vector.keys, offset);
                    valueConverter.convert(entry.getValue(), vector.values, offset);
                }
            }
        }
    }
}
