package operations;


import com.google.api.services.bigquery.model.TableRow;
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

public class EventProtoToJSONParser<OriginalT> extends PTransform<PCollection<FailSafeElement<OriginalT, byte[]>>, PCollectionTuple> {

    private static final Logger LOG = LoggerFactory.getLogger(EventProtoToJSONParser.class);

    private final TupleTag<FailSafeElement<OriginalT, byte[]>> DLQTag;  // garbage data, those not complying to validation rules
    private final TupleTag<FailSafeElement<OriginalT, TableRow>> mainTag; // Guaranteed to be insertable in BigQuery

    public EventProtoToJSONParser(TupleTag<FailSafeElement<OriginalT, TableRow>> mainTag, TupleTag<FailSafeElement<OriginalT, byte[]>> DLQTag) {
        this.DLQTag = DLQTag;
        this.mainTag = mainTag;
    }

    @Override
    public PCollectionTuple expand(PCollection<FailSafeElement<OriginalT, byte[]>> input) {
        return input.apply("MapToJSON",
                ParDo.of(new MapProtoEventToJSON())
                        .withOutputTags(mainTag, TupleTagList.of(DLQTag)));
    }

    private class MapProtoEventToJSON extends DoFn<FailSafeElement<OriginalT, byte[]>, FailSafeElement<OriginalT, TableRow>> {

        @ProcessElement
        public void processElement(@Element FailSafeElement<OriginalT, byte[]> input, ProcessContext c) {

            try {
                /*EventOuterClass.Event ev = EventOuterClass.Event.parseFrom(input.getPayload());
                for(TableRow row: EventParser.convertToTableRow(ev)) {
                    c.output(new FailSafeElement<>(input.getOriginalPayload(), row));
                }*/

            } catch (Exception e) {
                LOG.info(e.getMessage());
                c.output(DLQTag, input);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        TableRow tr = new TableRow();
        tr.set("F", "F");

        TableRow tr2 = new TableRow();
        tr2.set("E", "E");

        for(String key: tr.keySet()) {
            tr2.set(key, tr.get(key));
        }

        System.out.println(tr2.toString());
    }
}
