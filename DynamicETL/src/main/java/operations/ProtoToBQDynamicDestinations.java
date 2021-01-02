package operations;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;

public class ProtoToBQDynamicDestinations<InputT> extends PTransform<PCollection<InputT>, WriteResult> {

    private final SerializableFunction<InputT, String> destinationExtractor;
    private final SerializableFunction<InputT, TableRow> tableRowExtractor;

    public ProtoToBQDynamicDestinations(SerializableFunction<InputT, String> destinationExtractor, SerializableFunction<InputT, TableRow> tableRowExtractor) {
        this.destinationExtractor = destinationExtractor;
        this.tableRowExtractor = tableRowExtractor;
    }

    @Override
    public WriteResult expand(PCollection<InputT> input) {
        return input.apply(BigQueryIO.<InputT>write()
                .to(new DynamicDestinations<InputT, String>() {
                    @Override
                    public String getDestination(ValueInSingleWindow<InputT> element) {
                        return destinationExtractor.apply(element.getValue());
                    }

                    @Override
                    public TableDestination getTable(String destination) {
                        return new TableDestination(
                                new TableReference()
                                .setDatasetId(destination)
                                .setTableId("FetchFromProto"), null);
                    }

                    @Override
                    public TableSchema getSchema(String destination) {
                        return null;
                    }
                }));
    }
}
