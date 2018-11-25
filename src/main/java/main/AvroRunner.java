package main;

import org.apache.avro.specific.SpecificRecordBase;
import reader.AvroReader;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class AvroRunner {

    private Map<String, Class<? extends SpecificRecordBase>> avroFiles;

    public AvroRunner(Map<String, Class<? extends SpecificRecordBase>> avroFiles) {
        this.avroFiles = avroFiles;
    }

    public void run() {
        AvroReader avroReader = new AvroReader();

        avroFiles.forEach((filename, clazz) -> {
            try {
                avroReader.readFile(new File(filename), clazz);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
