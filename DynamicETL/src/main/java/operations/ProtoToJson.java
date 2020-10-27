package operations;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ProtoToJson<OriginalT> extends PTransform<PCollection<FailSafeElement<OriginalT, byte[]>>, PCollectionTuple> {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoToJson.class);

    private final TupleTag<FailSafeElement<OriginalT, String>> mainTag;
    private final TupleTag<FailSafeElement<OriginalT, byte[]>> DLQTag;
    private final Class protoClass;

    public ProtoToJson(TupleTag<FailSafeElement<OriginalT, String>> mainTag, TupleTag<FailSafeElement<OriginalT, byte[]>> DLQTag, Class protoClass) {
        this.mainTag = mainTag;
        this.DLQTag = DLQTag;
        this.protoClass = protoClass;
    }

    @Override
    public PCollectionTuple expand(PCollection<FailSafeElement<OriginalT, byte[]>> input) {
        return input.apply("MapToJSON",
                ParDo.of(new MapPubSubMessageToProto())
                        .withOutputTags(mainTag, TupleTagList.of(DLQTag)));
    }

    private class MapPubSubMessageToProto extends DoFn<FailSafeElement<OriginalT, byte[]>, FailSafeElement<OriginalT, String>> {

        private transient Method parseFn;

        @Setup
        public void setUp() {
            try {
                parseFn = protoClass.getDeclaredMethod("parseFrom", byte[].class);
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        @ProcessElement
        public void processElement(@Element FailSafeElement<OriginalT, byte[]> input, ProcessContext c) {
            try {
                LOG.info(JsonFormat.printer().print((MessageOrBuilder) parseFn.invoke(null, input.getPayload())));
                c.output(mainTag, new FailSafeElement<>(input.getOriginalPayload(), JsonFormat.printer().print((MessageOrBuilder) parseFn.invoke(null, input.getPayload()))));
            } catch (InvalidProtocolBufferException e) {
                c.output(DLQTag, input);
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }
}