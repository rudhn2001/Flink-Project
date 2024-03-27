package apis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ApiLogInput {
    public static void main(String[] args) {
        try {

            // Read the API raw log from the JSON file
            String logData = readApiRawLogFromFile("api_log.json");

            // Prompt the user for the template
            
            System.out.println("Enter the Key:");
            String keyString = promptTemplate();

            System.out.println("Enter the Value:");
            String valueString = promptTemplate();
            // Process the template
            String processedkeyString = processTemplate(keyString, logData);
            String processedvalueString = processTemplate(valueString, logData);
            System.out.println("Key : "+ processedkeyString + "\n" + "Value : "+ processedvalueString);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Read API raw log from JSON file
    private static String readApiRawLogFromFile(String filePath) throws IOException {
        StringBuilder logData = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                logData.append(line);
            }
        }
        return logData.toString();
    }

    // Prompt user for the template
    private static String promptTemplate() {
        Scanner scanner = new Scanner(System.in);

        return scanner.nextLine();
    }

    // Process the template string to replace {{}} fields with corresponding values from the API log
    private static String processTemplate(String templateString, String logData) throws IOException {
        Pattern pattern = Pattern.compile("\\{\\{\\s*(.*?)\\s*}}");
        Matcher matcher = pattern.matcher(templateString);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String fieldName = matcher.group(1);
            String fieldValue = extractFieldValue(fieldName, logData);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(fieldValue));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    // Extract field value from the API log based on the field name
    private static String extractFieldValue(String fieldName, String logData) throws IOException {
        // Parse JSON log data
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(logData);

        // Traverse the JSON structure to extract field value
        JsonNode valueNode = traverseJson(rootNode, fieldName);
        if (valueNode != null) {
            return valueNode.toString(); // Return JSON string representation of the field value
        } else {
            return ""; // Return empty string if field not found
        }
    }

    // Helper method to traverse JSON structure recursively and extract field value
    private static JsonNode traverseJson(JsonNode node, String fieldName) {
        String[] fields = fieldName.split("/");
        JsonNode currentNode = node;
        for (String field : fields) {
            currentNode = currentNode.get(field);
            if (currentNode == null) {
                return null; // Return null if any field is missing
            }
        }
        return currentNode;
    }
}
