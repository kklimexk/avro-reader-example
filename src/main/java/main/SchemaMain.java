package main;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;

public class SchemaMain {

    public static void main(String[] args) throws IOException {
        String res = getAvroSchemaVersionFromEvent("/home/kklimek/IdeaProjects/avro-reader-example/twitter.avro");
        System.out.println(res);
    }

    private static String getAvroSchemaVersionFromEvent(String pathToAvroFile) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<>(new File("/home/kklimek/IdeaProjects/avro-reader-example/twitter.avro"), datumReader);
        Schema schema = dataFileReader.getSchema();

        JsonElement el = new JsonParser().parse(schema.toString()).getAsJsonObject().get("namespace");
        return el.toString();
    }
}
