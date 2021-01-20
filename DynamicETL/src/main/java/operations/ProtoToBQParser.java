package operations;

import com.google.api.services.bigquery.model.*;
import lvi.BQParserImp;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProtoToBQParser<InputT> extends PTransform<PCollection<InputT>, PDone> {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoToBQParser.class);

    private final SerializableFunction<InputT, String> protoTypeExtractor;
    private final SerializableFunction<InputT, byte[]> protoPayloadExtractor;
    private final SerializableFunction<InputT, String> datasetExtractor;

    public ProtoToBQParser(SerializableFunction<InputT, String> protoTypeExtractor,
                           SerializableFunction<InputT, String> datasetExtractor,
                           SerializableFunction<InputT, byte[]> protoPayloadExtractor) {

        this.protoTypeExtractor = protoTypeExtractor;
        this.datasetExtractor = datasetExtractor;
        this.protoPayloadExtractor = protoPayloadExtractor;
    }

    @Override
    public PDone expand(PCollection<InputT> input) {

        // Apply ParDo
        input
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
                .apply("ProtoToBQ", ParDo.of(new ProtoToBQ()))
                .apply(BigQueryIO.<KV<KV<TableDestination, String>, TableRow>>write()
                        .to(new ProtoToBQDynamicDestinations())
                        .withFormatFunction((SerializableFunction<KV<KV<TableDestination, String>, TableRow>, TableRow>) el -> {
                            // FUTURE: A shame this function can't be used to return multiple TableRows
                            return el.getValue();
                        })
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_EMPTY)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        return PDone.in(input.getPipeline());
    }

    private class ProtoToBQ extends DoFn<InputT, KV<KV<TableDestination, String>, TableRow>> {

        @ProcessElement
        public void processElement(@Element InputT input, ProcessContext c) {

            try {

                // Retrieve parser for type
                String protoType = protoTypeExtractor.apply(input);
                BQParserImp parser = BQParserImp.getParserForType(protoType);

                // Extract rows and destination
                for(TableRow row: parser.convertToTableRow(protoPayloadExtractor.apply(input))) {

                    LOG.info(row.toPrettyString());

                    String tableRef = "geometric-ocean-284614:" + datasetExtractor.apply(input) + "." + parser.getBigQueryTableName();

                    // Generate output object
                    c.output(KV.of(
                            KV.of(new TableDestination(tableRef,
                                            parser.getBigQueryTableDescription(),
                                            parser.getPartitioning(),
                                            parser.getClustering()),
                                    protoType),
                            row));
                }
            } catch (Exception e) {
                LOG.info("Error " + e.getMessage());

                // FUTURE: Add DLQ path to pipeline
                // c.output(DLQTag, new FailSafeElement<>(input, e.getMessage()));
            }
        }
    }

    private class ProtoToBQDynamicDestinations extends DynamicDestinations<KV<KV<TableDestination, String>, TableRow>, KV<TableDestination, String>> {

        @Override
        public KV<TableDestination, String> getDestination(ValueInSingleWindow<KV<KV<TableDestination, String>, TableRow>> element) {
            return element.getValue().getKey();
        }

        @Override
        public TableDestination getTable(KV<TableDestination, String> destination) {
            return destination.getKey();
        }

        @Override
        public TableSchema getSchema(KV<TableDestination, String> destination) {
            try {
                // TableSchemas are not serializable by default, hence we obtain it here
                return BQParserImp.getParserForType(destination.getValue()).getBigQueryTableSchema();
            } catch (Exception e) {
                throw new RuntimeException();
            }
        }
    }
}
