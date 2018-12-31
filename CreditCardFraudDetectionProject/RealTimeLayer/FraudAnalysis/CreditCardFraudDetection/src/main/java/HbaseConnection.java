import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;

public class HbaseConnection implements Serializable {
	private static final long serialVersionUID = 1L;
	static Admin hbaseAdmin = null;
	@SuppressWarnings("deprecation")
	public static HTable GetHbaseTableConnections(String hbaseip, String tableName) throws IOException {
		org.apache.hadoop.conf.Configuration conf =
				(org.apache.hadoop.conf.Configuration) HBaseConfiguration.create();
		conf.setInt("timeout", 1200);
		conf.set("hbase.master", hbaseip + ":60000");
		conf.set("hbase.zookeeper.quorum", hbaseip);
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase");
		Connection con = ConnectionFactory.createConnection(conf);
		try {
			if (hbaseAdmin == null) {
				hbaseAdmin = con.getAdmin();
			}
			return new HTable(hbaseAdmin.getConfiguration(), tableName);
		} catch (Exception e) {
				e.printStackTrace();
		}
		return null;
	}
}
