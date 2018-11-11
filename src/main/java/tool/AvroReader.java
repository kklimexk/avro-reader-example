package tool;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

public class AvroReader {

    private static Logger logger = Logger.getLogger("AvroReader");

    public <T extends SpecificRecordBase> void readFile(File file, Class<T> avroClass)
            throws IOException {

        logger.info("Reading avro file: " + file.getAbsolutePath() +
                " with avro generated class: " + avroClass.getName() + "\n");

        SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(avroClass);

        try (DataFileReader<T> fileReader = new DataFileReader<>(file, datumReader)) {
            List<Schema.Field> fields = fileReader.getSchema().getFields();
            System.out.println("All fields: " + fields + "\n");

            while (fileReader.hasNext()) {
                T record = fileReader.next();
                System.out.println(record);
            }
        } catch (org.apache.avro.AvroTypeException ex) {
            ex.printStackTrace();
        }
    }
}
