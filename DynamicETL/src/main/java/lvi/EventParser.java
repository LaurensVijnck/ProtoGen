// Generated by the proto-to-bq Proto compiler plugin.  DO NOT EDIT!
package lvi; 

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableCell;

import java.util.LinkedList;
import java.util.List;

public final class EventParser {

	public static List<TableRow> convertToTableRow(EventOuterClass.Event event) throws Exception {
		List<TableRow> eventsRows = new LinkedList<>();
		
		for(EventOuterClass.BatchEvent batchEvent: event.getEventsList()) {
		
			TableRow row = new TableRow();

			// Client
			if(event.hasClient()) {
				if(event.getClient().hasTenantId()) {
					row.set("tenantId", event.getClient().getTenantId());
				} else {
					throw new Exception();
				} 

				row.set("name", event.getClient().getName());
			} 


			// Actor
			TableCell actorCell = new TableCell();
			if(batchEvent.hasActor()) {
				if(batchEvent.getActor().hasUserId()) {
					actorCell.set("userId", batchEvent.getActor().getUserId());
				} else {
					throw new Exception();
				} 

				actorCell.set("email", batchEvent.getActor().getEmail());

				// Address
				TableCell addressCell = new TableCell();
				if(batchEvent.getActor().hasAddress()) {
					addressCell.set("street", batchEvent.getActor().getAddress().getStreet());
					addressCell.set("number", batchEvent.getActor().getAddress().getNumber());
					addressCell.set("country", batchEvent.getActor().getAddress().getCountry());
				} 

				actorCell.set("address", addressCell);
			} else {
				throw new Exception();
			} 

			row.set("actor", actorCell);
			eventsRows.add(row);
		}

		return eventsRows;
	}
}