package pipelines;

import operations.ProtoToBQParser;
import org.apache.beam.runners.dataflow.DataflowRunner;
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
        System.out.println(options.getProject());

        // FUTURE: Supply via options
        options.setTempLocation("gs://dataflow-staging-us-central1-230586129391/temp");
        options.setRunner(DataflowRunner.class);
        options.setProject("geometric-ocean-284614");
        options.setJobName("DynamicETL-v1");
        options.setUpdate(false);
        options.setTemplateLocation("gs://dev-lvi-templates/proto-to-bq/v1/template");

        // Create pipeline object
        Pipeline p = Pipeline.create(options);

        // Read input
        p
                .apply("ReadInput", PubsubIO
                    .readMessagesWithAttributes()
                    .fromSubscription(options.getPubSubInputSubscription()))

                // The cool thing about this technique, is that the pipeline is able to process
                // different sources simultaneously. If they, for example, originate from different
                // topics, one could union the streams from these topics and alter the protoTypeExtractor
                // of the ProtoBQParser. e.g.,
                //
                //  1. Consume stream A, map onto KV<PubSubMessage, String ("A")>
                //  2. Consume stream B, map onto KV<PubSubMessage, String ("B")>
                //  3. Union streams above
                //  4. Pass function that extracts the value from the KVs constructed above as the ProtoTypeExtractor of ProtoBQParser
                //
                .apply(new ProtoToBQParser<>(
                        options.getProject(),
                        options.getEnvironment(),
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

    /**
     *
     */
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
