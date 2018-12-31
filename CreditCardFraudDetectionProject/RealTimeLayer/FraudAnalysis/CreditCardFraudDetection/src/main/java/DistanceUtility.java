import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Utility class that reads file zipCodePosId.csv and using same if two zip
 * codes are provided, it returns distances.
 *
 */

class ZipCodeData {
	double lat;
	double lon;
	String city;
	String state_name;
	String postId;

	public ZipCodeData(double lat, double lon, String city, String state_name, String postId) {
		this.lat = lat;
		this.lon = lon;
		this.city = city;
		this.state_name = state_name;
		this.postId = postId;
	}
}

class DistanceUtility implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	static HashMap<String, ZipCodeData> zipCodesMap = new HashMap<String, ZipCodeData>();

	/**
	 * Initialize zip codes using given file
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public static void InitDistanceUtility(String filePath) throws NumberFormatException, IOException {

		BufferedReader br = new BufferedReader(new FileReader(filePath));

		String line = null;

		while ((line = br.readLine()) != null) {
			String str[] = line.split(",");

			String zipCode = str[0];

			double lat = Double.parseDouble(str[1]);
			double lon = Double.parseDouble(str[2]);
			;
			String city = str[3];
			String state_name = str[4];
			String postId = str[5];

			ZipCodeData zipCodeData = new ZipCodeData(lat, lon, city, state_name, postId);

			zipCodesMap.put(zipCode, zipCodeData);
		}
	}

	/**
	 *
	 * @param zipcode1
	 *            - zip code of previous transaction
	 * @param zipcode2
	 *            - zip code of current transaction
	 * @return distance between two zip codes
	 */
	public static double getDistanceViaZipCode(String zipcode1, String zipcode2) {
		ZipCodeData z1 = zipCodesMap.get(zipcode1);
		ZipCodeData z2 = zipCodesMap.get(zipcode2);
		return distance(z1.lat, z1.lon, z2.lat, z2.lon);
	}

	private static double distance(double lat1, double lon1, double lat2, double lon2) {
		double theta = lon1 - lon2;
		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2))
				+ Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		dist = dist * 60 * 1.1515;
		dist = dist * 1.609344;

		return dist;
	}

	private static double rad2deg(double rad) {
		return rad * 180.0 / Math.PI;
	}

	private static double deg2rad(double deg) {
		return deg * Math.PI / 180.0;
	}

}