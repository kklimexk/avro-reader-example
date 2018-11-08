package com.example;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;

public class Main {

    private final static String AVRO_FILE = "unionrecord.avro";

    public static void main(String[] args)
            throws IOException {
        File file = new File(AVRO_FILE);
        readFile(file);
    }

    private static void readFile(File file)
            throws IOException {

        SpecificDatumReader<TestUnionNested> datumReader = new SpecificDatumReader<>(TestUnionNested.class);

        DataFileReader<TestUnionNested> fileReader = new DataFileReader<>(file, datumReader);
        TestUnionNested record = fileReader.next();

        System.out.println(record);

        fileReader.close();
    }
}
