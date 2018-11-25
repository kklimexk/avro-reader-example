package main;

import org.apache.avro.specific.SpecificRecordBase;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws ClassNotFoundException {
        if (args.length <= 1) throw new IllegalArgumentException("No paths to avros provided!");
        else if (args.length % 2 == 0) {

            Map<String, Class<? extends SpecificRecordBase>> avroFiles = new HashMap<>();

            for (int i = 0; i < args.length - 1; i += 2) {
                avroFiles.put(args[i], Class.forName(args[i + 1]).asSubclass(SpecificRecordBase.class));
            }

            AvroRunner avroRunner = new AvroRunner(avroFiles);
            avroRunner.run();
        } else {
            throw new IllegalArgumentException("You have to specify path to avro with avro generated class!");
        }
    }
}
