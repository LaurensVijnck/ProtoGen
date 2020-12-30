package operations;

import com.google.api.services.bigquery.model.TableRow;
import lvi.BQParserImp;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProtoToBQParser<InputT> extends PTransform<PCollection<InputT>, PCollectionTuple> {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoToBQParser.class);

    private final TupleTag<InputT> DLQTag;
    private final TupleTag<TableRow> mainTag;

    private final SerializableFunction<InputT, String> protoTypeExtractor;
    private final SerializableFunction<InputT, byte[]> protoPayloadExtractor;

    public ProtoToBQParser(TupleTag<TableRow> mainTag, TupleTag<InputT> DLQTag, SerializableFunction<InputT, String> protoTypeExtractor, SerializableFunction<InputT, byte[]> protoPayloadExtractor) {
        this.mainTag = mainTag;
        this.DLQTag = DLQTag;
        this.protoTypeExtractor = protoTypeExtractor;
        this.protoPayloadExtractor = protoPayloadExtractor;
    }

    @Override
    public PCollectionTuple expand(PCollection<InputT> input) {
        return input.apply("MapToJSON",
                ParDo.of(new ProtoToBQ()).withOutputTags(mainTag, TupleTagList.of(DLQTag)));
    }

    private class ProtoToBQ extends DoFn<InputT, TableRow> {

        @ProcessElement
        public void processElement(@Element InputT input, ProcessContext c) {

            try {
                for(TableRow row: BQParserImp
                        .getParserForType(protoTypeExtractor.apply(input))
                        .convertToTableRow(protoPayloadExtractor.apply(input))) {

                    c.output(mainTag, row);
                    LOG.info(row.toPrettyString());
                }
            } catch (Exception e) {
                c.output(DLQTag, input);
            }
        }
    }
}
