package pipelines;

import common.operations.GenericPrinter;
import models.FailSafeElement;
import models.coders.FailSafeElementCoder;
import operations.EventProtoToJSONParser;
import operations.JsonToTableRowConverter;
import operations.WriteJSONToBigQuery;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipelines.config.DynamicETLPipelineOptions;

public class DynamicETL {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicETL.class);

    private static final TupleTag<FailSafeElement<PubsubMessage, byte[]>> PROTO_DLQ = new TupleTag<>() {};
    private static final TupleTag<FailSafeElement<PubsubMessage, String>> PROTO_MAIN = new TupleTag<>() {};

    public static void main(String[] args) {

        // Parse arguments into options
        DynamicETLPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .create()
                .as(DynamicETLPipelineOptions.class);

        System.out.println(options.getPubSubInputSubscription());

        // Create pipeline object
        Pipeline p = Pipeline.create(options);


        // Read input
        PCollectionTuple input = p
                .apply("ReadInput", PubsubIO
                    .readMessagesWithAttributes()
                    .fromSubscription(options.getPubSubInputSubscription()))
                .apply("MapToBytes", MapElements.via(new PubSubBytesConverter()))
                    .setCoder(FailSafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), ByteArrayCoder.of()))
                .apply(new EventProtoToJSONParser<>(PROTO_MAIN, PROTO_DLQ));

        // Write success events
        input
                .get(PROTO_MAIN).setCoder(FailSafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of()))
                .apply(new WriteJSONToBigQuery<>(new JsonToTableRowConverter<>(), options.getOutputTable(), options.getDeadLetterTable()));


        // Write failed events
        input
                .get(PROTO_DLQ).setCoder(FailSafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), ByteArrayCoder.of()))
                .apply(ParDo.of(new GenericPrinter<>()));

        p.run();
    }

    private static class PubSubBytesConverter extends SimpleFunction<PubsubMessage, FailSafeElement<PubsubMessage, byte[]>> {

        @Override
        public FailSafeElement<PubsubMessage, byte[]> apply(PubsubMessage input) {
            return new FailSafeElement<>(input, input.getPayload());
        }
    }
}
