import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement; 


/**
 * Allows clients to query and update the database in order to log in, search
 * for flights, reserve seats, show reservations, and cancel reservations.
 */
public class FlightsDB {

  /** Maximum number of reservations to allow on one flight. */
  private static int MAX_FLIGHT_BOOKINGS = 3;

  /** Holds the connection to the database. */
  private Connection conn;

  /** Opens a connection to the database using the given settings. */
  public void open(Properties settings) throws Exception {
    // Make sure the JDBC driver is loaded.
    String driverClassName = settings.getProperty("flightservice.jdbc_driver");
    Class.forName(driverClassName).newInstance();

    // Open a connection to our database.
    conn = DriverManager.getConnection(
        settings.getProperty("flightservice.url"),
        settings.getProperty("flightservice.sqlazure_username"),
        settings.getProperty("flightservice.sqlazure_password"));
  }

  /** Closes the connection to the database. */
  public void close() throws SQLException {
    conn.close();
    conn = null;
  }

  // SQL statements with spaces left for parameters:
  private PreparedStatement beginTxnStmt;
  private PreparedStatement commitTxnStmt;
  private PreparedStatement abortTxnStmt;
  private PreparedStatement searchOneHopStmt;
  private PreparedStatement searchTwoHopStmt;
  private PreparedStatement loginStmt;
  private PreparedStatement showResStmt;
  private PreparedStatement cancelStmt;
  private PreparedStatement checkResNum;
  private PreparedStatement checkSameDay;
  private PreparedStatement bookFlight;
  
  private static final String DIRECT_SQL = 
		  "SELECT TOP (99) fid, name, flight_num, origin_city, dest_city, actual_time " +
	      "FROM Flights F1, Carriers " +
		  "WHERE carrier_id = cid AND actual_time IS NOT NULL AND " +
		  "    year = ? AND month_id = ? AND day_of_month = ? AND origin_city = ? AND dest_city = ? " +
		  "ORDER BY actual_time ASC";
  private static final String TWO_HOP_SQL = 
	"SELECT TOP (99) F1.fid as fid1, C1.name as name1, " +
			"    F1.flight_num as flight_num1, F1.origin_city as origin_city1, " +
			"    F1.dest_city as dest_city1, F1.actual_time as actual_time1, " +
			"    F2.fid as fid2, C2.name as name2, " +
			"    F2.flight_num as flight_num2, F2.origin_city as origin_city2, " +
			"    F2.dest_city as dest_city2, F2.actual_time as actual_time2\n" +
	"FROM Flights F1, Flights F2, Carriers C1, Carriers C2\n" +
    "WHERE F1.carrier_id = C1.cid AND F1.actual_time IS NOT NULL AND " +
      	"    F2.carrier_id = C2.cid AND F2.actual_time IS NOT NULL AND " +
  		"    F1.day_of_month = ? AND F1.month_id = ? AND F1.year = ? AND F1.origin_city = ? AND F2.dest_city = ? AND" +
  		"    F1.dest_city = F2.origin_city AND F1.month_id = F2.month_id AND F1.year = F2.year AND F1.day_of_month = F2.day_of_month " +
  	"ORDER BY F1.actual_time + F2.actual_time ASC";
  private static final String LOGIN_SQL = 
		  "SELECT uid, handle, fullName " + 
		  "FROM Customer C " +
		  "WHERE C.handle = ? AND C.pssword = ?";
  private static final String SHOW_SQL = 
		  "SELECT F1.fid as flightid, year, month_id, day_of_month, name, flight_num, origin_city, dest_city, actual_time " +
		  "FROM Flights F1, Carriers, Reservation R1 " +
		  "WHERE carrier_id = cid AND actual_time IS NOT NULL AND F1.fid = R1.fid AND R1.uid = ?";
  private static final String CANCLE_SQL = "DELETE FROM Reservation " +
		  								   "WHERE uid = ? AND fid = ?";
  private static final String RES_NUM = "SELECT COUNT(*) AS passenger FROM Reservation WHERE fid = ?";
  private static final String SAME_DAY = "SELECT day_of_month, month_id, year " + 
  										 "FROM Flights F1, Reservation R1 " +
  										 "WHERE uid = ? AND R1.fid = F1.fid";
  private static final String RESERVE_SQL = "INSERT INTO Reservation VALUES (?, ?)";
  
  /** Performs additional preparation after the connection is opened. */
  public void prepare() throws SQLException {
    // NOTE: We must explicitly set the isolation level to SERIALIZABLE as it
    //       defaults to allowing non-repeatable reads.
    beginTxnStmt = conn.prepareStatement(
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;");
    commitTxnStmt = conn.prepareStatement("COMMIT TRANSACTION");
    abortTxnStmt = conn.prepareStatement("ROLLBACK TRANSACTION");  
    searchOneHopStmt = conn.prepareStatement(DIRECT_SQL);
    searchTwoHopStmt = conn.prepareStatement(TWO_HOP_SQL);
    loginStmt = conn.prepareStatement(LOGIN_SQL);
    showResStmt = conn.prepareStatement(SHOW_SQL);
    cancelStmt = conn.prepareStatement(CANCLE_SQL);
    checkResNum = conn.prepareStatement(RES_NUM);
    checkSameDay = conn.prepareStatement(SAME_DAY);
    bookFlight = conn.prepareStatement(RESERVE_SQL);
    // TODO: create more prepared statements here
  }

  /**
   * Tries to log in as the given user.
   * @returns The authenticated user or null if login failed.
   */
  public User logIn(String handle, String password) throws SQLException {
	loginStmt.clearParameters();
	loginStmt.setString(1, handle);
	loginStmt.setString(2, password);
	ResultSet userinfo = loginStmt.executeQuery();
	// if user exists, then return a new User object with the information
	if (userinfo.next()) {
		User loggedIn = new User(userinfo.getInt("uid"), userinfo.getString("handle"), userinfo.getString("fullName"));
		userinfo.close();
		return loggedIn;
	}
    return null;
  }

  /**
   * Returns the list of all flights between the given cities on the given day.
   */
  public List<Flight[]> getFlights(
      int year, int month, int dayOfMonth, String originCity, String destCity)
      throws SQLException {

    List<Flight[]> results = new ArrayList<Flight[]>();
    // return direct flights
    searchOneHopStmt.clearParameters();
    searchOneHopStmt.setInt(1, year);
    searchOneHopStmt.setInt(2, month);
    searchOneHopStmt.setInt(3, dayOfMonth);
    searchOneHopStmt.setString(4, originCity);
    searchOneHopStmt.setString(5, destCity);
    ResultSet directResults = searchOneHopStmt.executeQuery();
    while (directResults.next()) {
      results.add(new Flight[] {
          new Flight(directResults.getInt("fid"), year, month, dayOfMonth,
              directResults.getString("name"),
              directResults.getString("flight_num"),
              directResults.getString("origin_city"),
              directResults.getString("dest_city"),
              (int)directResults.getFloat("actual_time"))
        });
    }
    directResults.close();
    // return two-hop flights
    searchTwoHopStmt.clearParameters();
    searchTwoHopStmt.setInt(1, dayOfMonth);
    searchTwoHopStmt.setInt(2, month);
    searchTwoHopStmt.setInt(3, year);
    searchTwoHopStmt.setString(4, originCity);
    searchTwoHopStmt.setString(5, destCity);
    ResultSet twoHopResults = searchTwoHopStmt.executeQuery();
    while (twoHopResults.next()) {
      results.add(new Flight[] {
          new Flight(twoHopResults.getInt("fid1"), year, month, dayOfMonth,
              twoHopResults.getString("name1"),
              twoHopResults.getString("flight_num1"),
              twoHopResults.getString("origin_city1"),
              twoHopResults.getString("dest_city1"),
              (int)twoHopResults.getFloat("actual_time1")),
          new Flight(twoHopResults.getInt("fid2"), year, month, dayOfMonth,
              twoHopResults.getString("name2"),
              twoHopResults.getString("flight_num2"),
              twoHopResults.getString("origin_city2"),
              twoHopResults.getString("dest_city2"),
              (int)twoHopResults.getFloat("actual_time2"))
        });
    }
    twoHopResults.close();
    return results;
  }

  /** Returns the list of all flights reserved by the given user. */
  public List<Flight> getReservations(int userid) throws SQLException {
    List<Flight> reservations = new ArrayList<Flight>();
    showResStmt.clearParameters();
    showResStmt.setInt(1, userid);
    ResultSet userReserved = showResStmt.executeQuery();
    while (userReserved.next()) {
    	reservations.add(new Flight(userReserved.getInt("flightid"), 
    		  userReserved.getInt("year"), 
    		  userReserved.getInt("month_id"), 
    		  userReserved.getInt("day_of_month"),
              userReserved.getString("name"),
              userReserved.getString("flight_num"),
              userReserved.getString("origin_city"),
              userReserved.getString("dest_city"),
              (int)userReserved.getFloat("actual_time"))
        );
    }
    userReserved.close();
  	return reservations;
  }

  /** Indicates that a reservation was added successfully. */
  public static final int RESERVATION_ADDED = 1;

  /**
   * Indicates the reservation could not be made because the flight is full
   * (i.e., 3 users have already booked).
   */
  public static final int RESERVATION_FLIGHT_FULL = 2;

  /**
   * Indicates the reservation could not be made because the user already has a
   * reservation on that day.
   */
  public static final int RESERVATION_DAY_FULL = 3;

  /**
   * Attempts to add a reservation for the given user on the given flights, all
   * occurring on the given day.
   * @returns One of the {@code RESERVATION_*} codes above.
   */
  public int addReservations(
      int userid, int year, int month, int dayOfMonth, List<Flight> flights)
      throws SQLException {
	  try{
		  beginTransaction();
		  checkSameDay.clearParameters();
		  checkSameDay.setInt(1, userid);
		  ResultSet dates = checkSameDay.executeQuery();
		  // check if the user already has a reservation on the same day
		  while (dates.next()) {
			  if (dates.getInt("day_of_month") == dayOfMonth && dates.getInt("month_id") == month 
					  && dates.getInt("year") == year) {
				  rollbackTransaction();
				  dates.close();
				  return RESERVATION_DAY_FULL;
			  }
		  }
		  // check if the flight has more than 3 users booked
		  for (int i = 0; i < flights.size(); i ++) {
			  checkResNum.clearParameters();
			  checkResNum.setInt(1, flights.get(i).id);
			  ResultSet passenger_num = checkResNum.executeQuery();
			  if (passenger_num.next()) {  
			  	if (passenger_num.getInt("passenger") >= 3) {
					  rollbackTransaction();
					  passenger_num.close();
					  return RESERVATION_FLIGHT_FULL;
				  }
			  }
		  }
		  // if both conditions are met, then book the list of flights given
		  for (int i = 0; i < flights.size(); i ++) {
			  bookFlight.clearParameters();
			  bookFlight.setInt(1, userid);
			  bookFlight.setInt(2, flights.get(i).id);
			  bookFlight.executeUpdate();
		  }
		  commitTransaction();
	  } catch (SQLException e) {
		  // return 0 indicates exception
		  try {
			  rollbackTransaction();
			  return 0;
		  } catch (SQLException se) {
			  return 0;
		  }
	  }
    return RESERVATION_ADDED;
  }

  /** Cancels all reservations for the given user on the given flights. */
  public void removeReservations(int userid, List<Flight> flights)
      throws SQLException {
	  beginTransaction();
	  for (int i = 0; i < flights.size(); i ++) {
		  cancelStmt.clearParameters();
		  cancelStmt.setInt(1, userid);
		  cancelStmt.setInt(2, flights.get(i).id);
		  cancelStmt.executeUpdate();
	  }
	  commitTransaction();
  }

  /** Puts the connection into a new transaction. */    
  public void beginTransaction() throws SQLException {
	conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    conn.setAutoCommit(false);  // do not commit until explicitly requested
    beginTxnStmt.executeUpdate();  
  }

  /** Commits the current transaction. */
  public void commitTransaction() throws SQLException {
    commitTxnStmt.executeUpdate(); 
    conn.setAutoCommit(true);  // go back to one transaction per statement
  }

  /** Aborts the current transaction. */
  public void rollbackTransaction() throws SQLException {
    abortTxnStmt.executeUpdate();
    conn.setAutoCommit(true);  // go back to one transaction per statement
  } 
}
