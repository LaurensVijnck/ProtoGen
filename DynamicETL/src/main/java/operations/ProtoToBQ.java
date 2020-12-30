package operations;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import lvi.BQParserImp;

public class ProtoToBQ<InputT> extends DoFn<InputT, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoToJson.class);

    private final SimpleFunction<InputT, String> protoTypeExtractor;
    private final SimpleFunction<InputT, byte[]> protoPayloadExtractor;

    public ProtoToBQ(SimpleFunction<InputT, String> protoTypeExtractor, SimpleFunction<InputT, byte[]> protoPayloadExtractor) {
        this.protoTypeExtractor = protoTypeExtractor;
        this.protoPayloadExtractor = protoPayloadExtractor;
        // this.protoRepository = new HashMap<>();
    }

    @ProcessElement
    public void processElement(@Element InputT input, ProcessContext c) {
        // BQParserImp.getParserForType("lvi").convertToTableRow()
    }


}
