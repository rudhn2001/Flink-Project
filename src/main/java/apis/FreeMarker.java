// package apis;


// import com.fasterxml.jackson.databind.JsonNode;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.fasterxml.jackson.databind.node.ObjectNode;
// import freemarker.template.Configuration;
// import freemarker.template.Template;
// import freemarker.template.TemplateException;

// import java.io.File;
// import java.io.IOException;
// import java.io.StringWriter;
// import java.util.HashMap;
// import java.util.Iterator;
// import java.util.Map;
// import java.util.Scanner;

// public class FreeMarker {

//     public static void main(String[] args) throws IOException, TemplateException {
//         // Create FreeMarker configuration
//         Configuration configuration = new Configuration(Configuration.VERSION_2_3_31);

//         // Prompt the user to input the template string
//         Scanner scanner = new Scanner(System.in);
//         System.out.println("Enter the template string:");
//         String templateString = scanner.nextLine();

//         // Read log data from sample.json file
//         Map<String, Object> logData = readJsonFile("sample.json");

//         // Render the template with actual values
//         String renderedTemplate = renderTemplate(templateString, logData);

//         // Output the rendered template
//         System.out.println("Rendered Template:");
//         System.out.println(renderedTemplate);

//         // Close the scanner
//         scanner.close();
//     }

//     private static Map<String, Object> readJsonFile(String filename) throws IOException {
//         // Initialize ObjectMapper
//         ObjectMapper mapper = new ObjectMapper();

//         // Read JSON file and parse it into a JsonNode
//         JsonNode rootNode = mapper.readTree(new File(filename));

//         // Convert JsonNode to Map<String, Object>
//         return convertJsonNodeToMap(rootNode);
//     }

//     private static Map<String, Object> convertJsonNodeToMap(JsonNode node) {
//         Map<String, Object> map = new HashMap<>();
//         if (node.isObject()) {
//             ObjectNode objectNode = (ObjectNode) node;
//             Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
//             while (fields.hasNext()) {
//                 Map.Entry<String, JsonNode> entry = fields.next();
//                 map.put(entry.getKey(), convertJsonNodeToMap(entry.getValue()));
//             }
//         } else if (node.isArray()) {
//             // Handle array case if needed
//         } else if (node.isValueNode()) {
//             map.put(node.asText(), null);
//         }
//         return map;
//     }

//     private static String renderTemplate(String templateString, Map<String, Object> dataModel) throws IOException, TemplateException {
//         // Create a Template object from the provided template string
//         Template template = new Template("template", templateString, new Configuration(Configuration.VERSION_2_3_31));

//         // Create a StringWriter to store the rendered output
//         StringWriter writer = new StringWriter();

//         // Render the template with the provided data model
//         template.process(dataModel, writer);

//         // Return the rendered template as a String
//         return writer.toString();
//     }
// }
