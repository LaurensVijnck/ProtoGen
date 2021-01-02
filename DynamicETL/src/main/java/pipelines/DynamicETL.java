package pipelines;

import operations.ProtoToBQParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipelines.config.DynamicETLPipelineOptions;

public class DynamicETL {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicETL.class);

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
        p
                .apply("ReadInput", PubsubIO
                    .readMessagesWithAttributes()
                    .fromSubscription(options.getPubSubInputSubscription()))
                .apply(new ProtoToBQParser<>(
                        new PubSubAttributeExtractor("proto_type"),
                        new PubSubAttributeExtractor("tenant_id"),
                        new PubSubBytesExtractor()));


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

    private static class PubSubBytesExtractor implements SerializableFunction<PubsubMessage, byte[]> {

        @Override
        public byte[] apply(PubsubMessage input) {
            return input.getPayload();
        }
    }
}
