package operations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import lvi.BigqueryOptions;
import lvi.Event;
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
                String JsonString = JsonFormat.printer().print(Event.EventBatch.parseFrom(input.getPayload()));

                for(String el: unBatchElements(JsonString)) {
                    LOG.info(el);
                    c.output(new FailSafeElement<>(input.getOriginalPayload(), el));
                }

            } catch (Exception e) {
                LOG.info(e.getMessage());
                c.output(DLQTag, input);
            }
        }

        private List<String> unBatchElements(String jsonString) throws JsonProcessingException {
            ObjectMapper jacksonObjMapper = new ObjectMapper();
            ObjectNode node = (ObjectNode)jacksonObjMapper.readTree(jsonString);
            ArrayNode batch = (ArrayNode)(node).remove(Event.EventBatch.getDescriptor().getOptions().getExtension(BigqueryOptions.batchField));
            List<String> elements = new LinkedList<>();

            for (Iterator<JsonNode> it = batch.elements(); it.hasNext(); ) {
                ObjectNode batchField = (ObjectNode)it.next();
                node.setAll(batchField);

                elements.add(jacksonObjMapper.writeValueAsString(node));
            }

            return elements;
        }

        private boolean isValid(String jsonString) {
            return false; // validate input event based on the schema inc. required/batch/oneof/
        }
    }
}
