package simpleSubh;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

public class monitor_csv_to_json {
    public static void main(String[] args) throws IOException {
        File file = new File("./kafka-basics/Givaudan_files/monitor_log.csv");
        CsvSchema csvSchema = CsvSchema.emptySchema().withHeader();
        CsvMapper csvMapper = new CsvMapper();
        MappingIterator<Map<?, ?>> mappingIterator =  csvMapper.reader().forType(Map.class).with(csvSchema).readValues(file);
        List<Map<?, ?>> list = mappingIterator.readAll();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode data = mapper.convertValue(list,JsonNode.class);
        mapper.writeValue(Files.newOutputStream(new File("./kafka-basics/monitor.json").toPath()),data);
    }
}
