package apis;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ApiLogInput {
    public static void main(String[] args) throws Exception {



        // Load API template from json
        List<String> fields = new ArrayList<>();
        Map<String, String> staticFields;
        try (FileInputStream fis = new FileInputStream("api_template.json")) {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(fis);
            JsonNode fieldsNode = jsonNode.get("fields");
            if (fieldsNode.isArray()) {
                Iterator<JsonNode> iterator = fieldsNode.elements();
                while (iterator.hasNext()) {
                    JsonNode fieldNode = iterator.next();
                    String fieldName = fieldNode.asText();
                    fields.add(fieldName);
                }
            }

            JsonNode staticFieldsNode = jsonNode.get("staticFields");
            staticFields = objectMapper.convertValue(staticFieldsNode, Map.class);
        }

        // GET THE API LOG FROM THE JSON FILE, CHANGE LATER TO DOCKER LINK
        String apiLogFilePath = "api_log.json";
        StringBuilder apiLogJsonBuilder = new StringBuilder();
        try (FileInputStream fis = new FileInputStream(apiLogFilePath)) {
            int character;
            while ((character = fis.read()) != -1) {
                apiLogJsonBuilder.append((char) character);
            }
        }

        String apiLogJson = apiLogJsonBuilder.toString();

        // PARSE THE JSON FROM API_LOG
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(apiLogJson);

        // FIELD EXTRACTION USING JSON FILE TEMPLATE
        for (String fieldName : fields) {
            
            // Find the field value in the JSON log based on field name
            JsonNode fieldValue = findFieldValue(jsonNode, fieldName);

            if (fieldValue != null) {
                System.out.println(fieldName + ": " + fieldValue.asText());
            } else {
                System.out.println("No value found for field: " + fieldName);
            }
        }

        // Print static fields
        for (Map.Entry<String, String> entry : staticFields.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    private static JsonNode findFieldValue(JsonNode jsonNode, String fieldName) {
        
        
        // Iterate through the JSON node to find the field value based on field name
        

        JsonNode fieldValue = null;
        for (JsonNode node : jsonNode) {
            if (node.has(fieldName)) {
                fieldValue = node.get(fieldName);
                break;
            }
        }
        return fieldValue;
    }
}
