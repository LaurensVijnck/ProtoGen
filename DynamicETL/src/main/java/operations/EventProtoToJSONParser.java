package operations;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.util.JsonFormat;
import lvi.BigqueryOptions;
import lvi.EventOuterClass;
import models.FailSafeElement;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class EventProtoToJSONParser<OriginalT> extends PTransform<PCollection<FailSafeElement<OriginalT, byte[]>>, PCollectionTuple> {

    private static final Logger LOG = LoggerFactory.getLogger(EventProtoToJSONParser.class);

    private final TupleTag<FailSafeElement<OriginalT, byte[]>> DLQTag;  // garbage data, those not complying to validation rules
    private final TupleTag<FailSafeElement<OriginalT, String>> mainTag; // Guaranteed to be insertable in BigQuery

    public EventProtoToJSONParser(TupleTag<FailSafeElement<OriginalT, String>> mainTag, TupleTag<FailSafeElement<OriginalT, byte[]>> DLQTag) {
        this.DLQTag = DLQTag;
        this.mainTag = mainTag;
    }

    @Override
    public PCollectionTuple expand(PCollection<FailSafeElement<OriginalT, byte[]>> input) {
        return input.apply("MapToJSON",
                ParDo.of(new MapProtoEventToJSON())
                        .withOutputTags(mainTag, TupleTagList.of(DLQTag)));
    }

    private class MapProtoEventToJSON extends DoFn<FailSafeElement<OriginalT, byte[]>, FailSafeElement<OriginalT, String>> {

        @ProcessElement
        public void processElement(@Element FailSafeElement<OriginalT, byte[]> input, ProcessContext c) {

            try {
                // String JsonString = JsonFormat.printer().print(Event.EventBatch.parseFrom(input.getPayload()));

                /*for (String el : unBatchElements(JsonString)) {
                    LOG.info(el);
                    c.output(new FailSafeElement<>(input.getOriginalPayload(), el));
                }*/

            } catch (Exception e) {
                LOG.info(e.getMessage());
                c.output(DLQTag, input);
            }
        }
    }

    private static List<String> unBatchElements(String jsonString, ArrayNode schema) throws Exception {
        /*ObjectMapper jacksonObjMapper = new ObjectMapper();
        ObjectNode node = (ObjectNode) jacksonObjMapper.readTree(jsonString);
        /// ArrayNode batch = (ArrayNode) (node).remove(EventOuterClass.Event.getDescriptor().getOptions().getExtension(BigqueryOptions.batchField));
        List<String> elements = new LinkedList<>();

        for (Iterator<JsonNode> it = batch.elements(); it.hasNext(); ) {
            ObjectNode batchField = (ObjectNode) it.next();
            node.setAll(batchField);

            if (validateElement(node, schema, "/")) {
                elements.add(jacksonObjMapper.writeValueAsString(node));
            }
        }

        return elements;*/
    }

    private static boolean validateElement(ObjectNode node, ArrayNode schema, String path) throws Exception {

        boolean allValid = true;
        // System.out.println(Arrays.toString(fields.stream().map(Descriptors.FieldDescriptor::getName).collect(Collectors.toList()).toArray()));
        Iterator<JsonNode> tableFields = schema.elements();
        JsonNode tableField;

        while (tableFields.hasNext()) {

            tableField = tableFields.next();

            if (tableField.get("mode").asText().equals("REQUIRED")) {
                JsonNode val = node.at(path + tableField.get("name").asText());
                // System.out.println(schema.toPrettyString());
                // System.out.println(node.toPrettyString());
                // System.out.println(path + tableField.get("name").asText() + ": " + val.isEmpty() + " " + val.asText().isEmpty());
                // System.out.println();

                if(val.isEmpty() && val.asText().isEmpty()) {
                    throw new Exception();
                }

            }

            JsonNode fields = tableField.get("fields");
            if (fields != null && fields.isArray()) {
                validateElement(node, (ArrayNode) tableField.get("fields"), path + tableField.get("name").asText() + "/");
            }
        }

        return allValid;
    }



    public List<TableRow> convertTableRow(Event.EventBatch eventBatch) {
        List<TableRow> rows = new LinkedList<>();

        // Client
        TableCell clientCell = new TableCell();
        if(eventBatch.hasClient()) {

            if(eventBatch.getClient().hasTenantId()) {
                clientCell.set("tenantId", eventBatch.getClient().getTenantId());
            }

            clientCell.set("name", eventBatch.getClient().getName());
        }

        for(Event.BatchEvent event: eventBatch.getEventsList()) {

            TableCell actorCell = new TableCell();
            if(event.hasActor()) {

                if(event.getActor().hasUserId()) {
                    actorCell.set("userId", event.getActor().getUserId());
                }

                clientCell.set("email", eventBatch.getClient().getName());

                TableCell addressCell = new TableCell();
                if(event.getActor().hasAddress()) {
                    addressCell.set("street", event.getActor().getAddress().getStreet());
                    addressCell.set("number", event.getActor().getAddress().getNumber());
                    addressCell.set("country", event.getActor().getAddress().getCountry());
                }

                clientCell.set("address", addressCell);
            }

            rows.add(new TableRow()
                    .set("client", clientCell)
                    .set("actor", actorCell));
        }

        return rows;
    }

    public static void main(String[] args) throws Exception {
        String jsonString = "{\n" +
                "  \"client\": {\n" +
                "    \"tenantId\": \"1337\",\n" +
                "    \"name\": \"LVI\"\n" +
                "  },\n" +
                "  \"events\": [\n" +
                "    {\n" +
                "      \"actor\": {\n" +
                "        \"userId\": \"80080\",\n" +
                "        \"email\": \"laurens@hotmail.com\",\n" +
                "        \"address\": {\n" +
                "          \"street\": \"Maastrichterpoort\",\n" +
                "          \"number\": \"2\",\n" +
                "          \"country\": \"Belgium\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String tableSchema = "[\n" +
                "  {\n" +
                "    \"description\": \"Owner of the event\",\n" +
                "    \"mode\": \"REQUIRED\",\n" +
                "    \"name\": \"client\",\n" +
                "    \"type\": \"RECORD\",\n" +
                "    \"fields\": [\n" +
                "      {\n" +
                "        \"description\": \"Identifier in the client catalog\",\n" +
                "        \"mode\": \"REQUIRED\",\n" +
                "        \"name\": \"tenantId\",\n" +
                "        \"type\": \"INTEGER\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"description\": \"\",\n" +
                "        \"mode\": \"NULLABLE\",\n" +
                "        \"name\": \"name\",\n" +
                "        \"type\": \"STRING\"\n" +
                "      }\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"description\": \"Actor concerned with the event\",\n" +
                "    \"mode\": \"REQUIRED\",\n" +
                "    \"name\": \"actor\",\n" +
                "    \"type\": \"RECORD\",\n" +
                "    \"fields\": [\n" +
                "      {\n" +
                "        \"description\": \"Identifier in the master table\",\n" +
                "        \"mode\": \"REQUIRED\",\n" +
                "        \"name\": \"userId\",\n" +
                "        \"type\": \"INTEGER\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"description\": \"Email address of the actor\",\n" +
                "        \"mode\": \"NULLABLE\",\n" +
                "        \"name\": \"email\",\n" +
                "        \"type\": \"STRING\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"description\": \"\",\n" +
                "        \"mode\": \"NULLABLE\",\n" +
                "        \"name\": \"address\",\n" +
                "        \"type\": \"RECORD\",\n" +
                "        \"fields\": [\n" +
                "          {\n" +
                "            \"description\": \"\",\n" +
                "            \"mode\": \"NULLABLE\",\n" +
                "            \"name\": \"street\",\n" +
                "            \"type\": \"STRING\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"description\": \"\",\n" +
                "            \"mode\": \"NULLABLE\",\n" +
                "            \"name\": \"number\",\n" +
                "            \"type\": \"STRING\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"description\": \"\",\n" +
                "            \"mode\": \"NULLABLE\",\n" +
                "            \"name\": \"country\",\n" +
                "            \"type\": \"STRING\"\n" +
                "          }\n" +
                "        ]\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "]";

        ObjectMapper jacksonObjMapper = new ObjectMapper();
        ObjectNode node = (ObjectNode) jacksonObjMapper.readTree(jsonString);
        System.out.println(node.toPrettyString());
        unBatchElements(jsonString, (ArrayNode) jacksonObjMapper.readTree(tableSchema));
    }
}
