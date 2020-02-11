package jheister;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.UUID;

class OrcPriceFile {
    private final Writer writer;
    private final VectorizedRowBatch batch;

    public OrcPriceFile(String name) throws IOException {
        Configuration conf = new Configuration();
        //todo: test compression
        //todo: effective time
        TypeDescription schema = TypeDescription.fromString("struct<price_date:date,id:string,close_price:double>");
        writer = OrcFile.createWriter(new Path(name),
                OrcFile.writerOptions(conf)
                        .overwrite(true)
                        .setSchema(schema));

        batch = schema.createRowBatch();
    }

    public void write(UUID id, LocalDate date, double price) throws IOException {
        LongColumnVector dates = (LongColumnVector) batch.cols[0];
        BytesColumnVector ids = (BytesColumnVector) batch.cols[1];
        DoubleColumnVector closePrice = (DoubleColumnVector) batch.cols[2];

        int row = batch.size++;
        closePrice.vector[row] = price;
        ids.setVal(row, id.toString().getBytes(StandardCharsets.UTF_8));
        dates.vector[row] = date.toEpochDay();
        // If the batch is full, write it out and start over.
        if (batch.size == batch.getMaxSize()) {
            writer.addRowBatch(batch);
            batch.reset();
        }
    }

    public void close() throws IOException {
        writer.close();
    }
}
