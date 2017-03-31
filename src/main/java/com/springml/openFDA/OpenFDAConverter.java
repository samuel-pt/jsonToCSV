package com.springml.openFDA;

import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by sam on 31/3/17.
 */
public class OpenFDAConverter {
    private String jsonLocation;
    private String dataNodeJsonPath;
    private String csvLocaton;
    private List<String> requiredColumns;

    public OpenFDAConverter(String jsonLocation, String dataNodeJsonPath,
                            String csvLocaton, List<String> requiredColumns) {
        this.jsonLocation = jsonLocation;
        this.dataNodeJsonPath = dataNodeJsonPath;
        this.csvLocaton = csvLocaton;
        this.requiredColumns = requiredColumns;
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
        System.out.println(records);
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
//        OpenFDAConverter openFDAFlattener = new OpenFDAConverter("/home/sam/work/projects/s/openFDA/data/sample.json",
//                "$.results", null, Arrays.asList(new String[] {"mdr_text.text_type_code", "manufacturer_link_flag", "device.brand_name"}));
        OpenFDAConverter openFDAFlattener = new OpenFDAConverter("/home/sam/work/projects/s/openFDA/data/device-event-0001-of-0002.json",
                "$.results", null, Arrays.asList(new String[] {"mdr_text.text_type_code", "manufacturer_link_flag", "device.brand_name"}));
        openFDAFlattener.convert();
    }
}
