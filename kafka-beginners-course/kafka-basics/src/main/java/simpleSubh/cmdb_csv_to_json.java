package simpleSubh;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.util.*;
import java.io.File;
import java.nio.file.Files;

public class cmdb_csv_to_json {
    public static void main(String[] args) throws IOException {
        File file = new File("./kafka-basics/Givaudan_files/cmdb.csv");
        CsvSchema csvSchema = CsvSchema.emptySchema().withHeader();
        CsvMapper csvMapper = new CsvMapper();
        MappingIterator<Map<?, ?>> mappingIterator =  csvMapper.reader().forType(Map.class).with(csvSchema).readValues(file);
        List<Map<?, ?>> list = mappingIterator.readAll();
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode arr = mapper.createArrayNode();
        for (Map<?,?> m:list) {
            if (m.get("Class").equals("Server")){
                JsonNode data = mapper.convertValue(m,JsonNode.class);
                System.out.println(data + "\n");
                arr.add(data);

            }
            mapper.writeValue(Files.newOutputStream(new File("cmdb.json").toPath()),arr);
        };
    }
}
