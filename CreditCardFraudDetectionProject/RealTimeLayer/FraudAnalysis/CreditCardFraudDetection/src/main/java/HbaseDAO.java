import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDAO {

	public static boolean getRowData(RowData row) {
		boolean status = false;
		try {
			Get g = new Get(row.getRowKey());
			Result result = row.getTable().get(g);
			if (result.isEmpty()) {
				return false;
			}
			Iterator<ColumnData> itr= row.getColInfo().iterator();  
			//Get all the col attributes
			while(itr.hasNext()){ 
				ColumnData col = itr.next();
				byte[] value = 
						result.getValue(Bytes.toBytes(col.getColFamily()),
								Bytes.toBytes(col.getColName()));
				if (value != null) {
					col.setValue(Bytes.toString(value));
				}
			}
			status = true;
		} catch (Exception e) {
			status = false;
			e.printStackTrace();
		}
		return status;
	}
	
	@SuppressWarnings("deprecation")
	public static boolean putRowData(RowData row) throws IOException {
		boolean status = false;
		try {
				
				Put p = new Put(row.getRowKey());
				Iterator<ColumnData> itr= row.getColInfo().iterator();
				while(itr.hasNext()) {
					ColumnData col = itr.next();
					p.addColumn(Bytes.toBytes(col.getColFamily()),
							Bytes.toBytes(col.getColName()),
							Bytes.toBytes(col.getValue()));
				}
				row.getTable().put(p);
				status = true;
		} catch (Exception e) {
			e.printStackTrace();
			status = false;
		} finally {
			
		}
		return status;
	}
}
