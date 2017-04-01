package com.springml.openFDA;

import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


/**
 * Created by sam on 31/3/17.
 */
public class OpenFDAConverter {
    private String jsonLocation;
    private String dataNodeJsonPath;
    private String csvLocation;
    private List<String> requiredColumns;

    public OpenFDAConverter(String jsonLocation, String dataNodeJsonPath,
                            String csvLocation, List<String> requiredColumns) {
        this.jsonLocation = jsonLocation;
        this.dataNodeJsonPath = dataNodeJsonPath;
        this.csvLocation = csvLocation;
        this.requiredColumns = requiredColumns;
    }

    public OpenFDAConverter(String jsonLocation, String dataNodeJsonPath,
                            String csvLocation, String requiredColumnsFileLocation) throws IOException {
        this(jsonLocation, dataNodeJsonPath, csvLocation, Arrays.asList(
                new String(Files.readAllBytes(Paths.get(requiredColumnsFileLocation))).split(",")));
    }

    public void convert() throws IOException {
        // Get the desired part of Json
        JSONArray jsonArray = JsonPath.parse(new File(jsonLocation)).read(dataNodeJsonPath);

        Iterator<Object> jsonIter = jsonArray.iterator();
        List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();

        while (jsonIter.hasNext()) {
            List<Map<String, Object>> subRecords = new ArrayList<Map<String, Object>>();
            Map<String, Object> currentRecord = new HashMap<String, Object>();
            subRecords.add(currentRecord);

            Map<String, Object> jsonObject  = (Map<String, Object>) jsonIter.next();
            addMap(null, subRecords, jsonObject);
            records.addAll(subRecords);
        }

        System.out.println(records.size());
        writeCSV(csvLocation, records);
        System.out.println(records);
    }

    private void writeCSV(String csvLocation, List<Map<String, Object>> records) throws IOException {
        FileWriter fileWriter = new FileWriter(csvLocation);

        CSVPrinter printer = CSVFormat.DEFAULT.withHeader(getHeader(records)).print(fileWriter);
        for (Map<String, Object> record: records) {
            printer.printRecord(record.values());
            printer.flush();
        }

        printer.close();
    }

    private String[] getHeader(List<Map<String, Object>> records) {
        List<Map<String, Object>> sampleRecords = getSampleRecords(records);
        Set<String> headers = new HashSet<String>();
        for (Map<String, Object> sampleRecord : sampleRecords) {
            Set<String> currentHeaders = sampleRecord.keySet();
            if (currentHeaders.size() > headers.size()) {
                headers = currentHeaders;
            }
        }

        return headers.toArray(new String[headers.size()]);
    }

    private List<Map<String, Object>> getSampleRecords(List<Map<String, Object>> records) {
        int sampleCount = 10;
        if (records.size() < sampleCount) {
            return records;
        }

        List<Map<String, Object>> sampleRecords = new ArrayList<Map<java.lang.String, Object>>();
        Random random = new Random();
        int totalSize = records.size();
        for (int i = 0; i < sampleCount; i++) {
            sampleRecords.add(records.get(random.nextInt(totalSize)));
        }

        return sampleRecords;
    }

    private void addMap(String parentKey, List<Map<String, Object>> subRecords, Map<String, Object> jsonObject) {
        Set<Map.Entry<String, Object>> entrySet = jsonObject.entrySet();
        Iterator<Map.Entry<String, Object>> entriesIter = entrySet.iterator();
        while (entriesIter.hasNext()) {
            Map.Entry<String, Object> entry = entriesIter.next();

            String key = entry.getKey();
            if (parentKey != null) {
                key = parentKey + "." + key;
            }
            Object value = entry.getValue();
            addValue(subRecords, key, value);
        }
    }

    private void addValue(List<Map<String, Object>> subRecords, String key, Object value) {
        if (value instanceof JSONArray) {
            addArray(key, subRecords, (JSONArray)value);
        } else if (value instanceof Map) {
            addMap(key, subRecords, (Map) value);
        } else {
            if (requiredColumns.contains(key)) {
                for (Map<String, Object> record : subRecords) {
                    record.put(key, value);
                }
            }
        }
    }

    private void addArray(String key, List<Map<String, Object>> records, JSONArray jsonArray) {
        Iterator iter = jsonArray.iterator();
        // Adding first entry as it is
        Object value = iter.next();
        addValue(records, key, value);

        // TODO - Handle other elements in array
        List<Map<String, Object>> subRecords = new ArrayList<Map<String, Object>>();
        while(iter.hasNext()) {
            value = iter.next();
            for (Map<String, Object> record : records) {
                Map<String, Object> newRecord = (Map<String, Object>) ((HashMap) record).clone();
                subRecords.add(newRecord);
                addValue(records, key, value);
            }
        }
        records.addAll(subRecords);
    }

    public static void main(String args[]) throws IOException {
        OpenFDAConverter openFDAFlattener = null;
        if (args.length == 0) {
            System.out.println("Running with Sample values");
            openFDAFlattener = new OpenFDAConverter("/home/sam/work/projects/s/openFDA/data/sample.json",
                    "$.results", "/home/sam/work/projects/s/openFDA/data/sample.csv",
                    Arrays.asList(new String[]{"mdr_text.text_type_code", "manufacturer_link_flag", "device.brand_name"}));
        } else if (args.length != 4) {
            System.out.println("Please provide jsonLocation, datanodeJsonPath, csvLocation and fieldsFileLocation");
            System.exit(1);
        } else {
            String jsonLocation = args[0];
            String datanodeJsonPath = args[1];
            String csvLocation = args[2];
            String fieldsFileLocation = args[3];
            System.out.println(jsonLocation);
            System.out.println(datanodeJsonPath);
            System.out.println(csvLocation);
            System.out.println(fieldsFileLocation);
            openFDAFlattener = new OpenFDAConverter(jsonLocation, datanodeJsonPath, csvLocation, fieldsFileLocation);
        }
//        OpenFDAConverter openFDAFlattener = new OpenFDAConverter("/home/sam/work/projects/s/openFDA/data/device-event-0001-of-0002.json",
//                "$.results", null, Arrays.asList(new String[] {"mdr_text.text_type_code", "manufacturer_link_flag", "device.brand_name"}));
        openFDAFlattener.convert();
    }
}
