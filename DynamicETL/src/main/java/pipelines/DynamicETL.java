package pipelines;

import com.google.api.services.bigquery.model.TableRow;
import common.operations.GenericPrinter;
import operations.ProtoToBQParser;
import operations.WriteJSONToBigQuery;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipelines.config.DynamicETLPipelineOptions;

public class DynamicETL {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicETL.class);

    private static final TupleTag<PubsubMessage> PROTO_DLQ = new TupleTag<>() {};
    private static final TupleTag<TableRow> PROTO_MAIN = new TupleTag<>() {};

    public static void main(String[] args) {

        // Parse arguments into options
        DynamicETLPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .create()
                .as(DynamicETLPipelineOptions.class);

        System.out.println(options.getPubSubInputSubscription());

        options.setTempLocation("gs://dataflow-staging-us-central1-230586129391/temp");

        // Create pipeline object
        Pipeline p = Pipeline.create(options);

        // Read input
        PCollectionTuple rows = p
                .apply("ReadInput", PubsubIO
                    .readMessagesWithAttributes()
                    .fromSubscription(options.getPubSubInputSubscription()))
                .apply(new ProtoToBQParser<>(PROTO_MAIN, PROTO_DLQ, new PubSubAttributeExtractor("proto_type"), new PubSubBytesConverter()));

        rows.get(PROTO_MAIN).apply(ParDo.of(new GenericPrinter<>()));
        rows.get(PROTO_DLQ).apply(ParDo.of(new GenericPrinter<>()));


//        // Write success events
//        input
//                .get(PROTO_MAIN).setCoder(FailSafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), TableRowJsonCoder.of()))
//                .apply(new WriteJSONToBigQuery<>(new IdentityConverter<>(), options.getOutputTable(), options.getDeadLetterTable()));
//
//
//        // Write failed events
//        input
//                .get(PROTO_DLQ).setCoder(FailSafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), ByteArrayCoder.of()))
//                .apply(ParDo.of(new GenericPrinter<>()));

        p.run();
    }

    public static class PubSubAttributeExtractor implements SerializableFunction<PubsubMessage, String> {

        private final String attribute;

        public PubSubAttributeExtractor(String attribute) {
            this.attribute = attribute;
        }

        @Override
        public String apply(PubsubMessage input) {
            return input.getAttribute(this.attribute);
        }
    }

    private static class PubSubBytesConverter implements SerializableFunction<PubsubMessage, byte[]> {

        @Override
        public byte[] apply(PubsubMessage input) {
            return input.getPayload();
        }
    }
}
