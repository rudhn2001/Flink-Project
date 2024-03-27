package apis;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class ApiLogInput {
    public static void main(String[] args) {
        try {
            // Initialize FreeMarker configuration
            Configuration cfg = new Configuration(Configuration.VERSION_2_3_31);

            // Read the API raw log from the JSON file
            String logData = readApiRawLogFromFile("api_log.json");

            // Prompt the user for the template
            Template template = promptTemplate(cfg);

            // Parse the log data
            Map<String, String> logMap = parseLog(logData);

            // Process the template
            StringWriter out = new StringWriter();
            template.process(logMap, out);
            System.out.println(out.toString());
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
    private static Template promptTemplate(Configuration cfg) throws IOException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter the template:");
        StringBuilder templateText = new StringBuilder();
        while (true) {
            System.out.print("Enter key (or 'done' to finish): ");
            String key = scanner.nextLine().trim();
            if (key.equalsIgnoreCase("done")) {
                break;
            }
            System.out.print("Enter value: ");
            String value = scanner.nextLine().trim();
            templateText.append(key).append("=").append(value).append("\n");
        }
        StringReader reader = new StringReader(templateText.toString());
        return new Template("userInputTemplate", reader, cfg);
    }

    // Parse log data into key-value pairs
    private static Map<String, String> parseLog(String logData) throws JSONException {
        Map<String, String> logMap = new HashMap<>();
        JSONObject jsonObject = new JSONObject(logData);
        flattenJson("", jsonObject, logMap);
        return logMap;
    }

    // Helper method to flatten JSON structure
    private static void flattenJson(String prefix, JSONObject json, Map<String, String> logMap) throws JSONException {
        for (String key : json.keySet()) {
            Object value = json.get(key);
            if (value instanceof JSONObject) {
                flattenJson(prefix + key + ".", (JSONObject) value, logMap);
            } else {
                logMap.put(prefix + key, value.toString());
            }
        }
    }
}
