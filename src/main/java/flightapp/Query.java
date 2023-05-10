package flightapp;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Runs queries against a back-end database
 */
public class Query extends QueryAbstract {
  //
  // Canned queries
  //
  private static final String FLIGHT_CAPACITY_SQL =
          "SELECT capacity FROM Flights WHERE fid = ?";
  private static final String MATCHING_USERNAME_SQL =
          "SELECT * FROM Users_ckirby03 WHERE LOWER(username) = LOWER(?)";
  private static final String ADD_USER_SQL =
          "INSERT INTO Users_ckirby03 VALUES (?, ?, ?)";
  private static final String FIND_DIRECT_ITINERARY_SQL =
          "SELECT fid, day_of_month, carrier_id, flight_num, origin_city, dest_city, actual_time, capacity, price \n" +
                  "FROM FLIGHTS AS f\n" +
                  "WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? AND canceled = 0\n" +
                  "ORDER BY actual_time, fid";

  private static final String FIND_INDIRECT_ITINERARY_SQL =
          "SELECT f1.fid AS f1_fid, f1.day_of_month AS f1_day_of_month, f1.carrier_id AS f1_carrier_id, " +
                  "f1.flight_num AS f1_flight_num, f1.origin_city AS f1_origin_city, " +
                  "f1.dest_city AS f1_dest_city, f1.actual_time AS f1_actual_time, f1.capacity AS f1_capacity, " +
                  "f1.price AS f1_price, f2.fid AS f2_fid, f2.day_of_month AS f2_day_of_month, " +
                  "f2.carrier_id AS f2_carrier_id, f2.flight_num AS f2_flight_num, f2.origin_city AS f2_origin_city, " +
                  "f2.dest_city AS f2_dest_city, f2.actual_time AS f2_actual_time, f2.capacity AS f2_capacity, " +
                  "f2.price AS f2_price," +
                  "f1.actual_time + f2.actual_time AS total_time" +
                  "FROM FLIGHTS AS f1" +
                  "JOIN FLIGHTS AS f2 ON f1.dest_city = f2.origin_city" +
                  "WHERE f1.origin_city = ? AND f2.dest_city = ? AND f1.day_of_month = ?" +
                  "AND f1.canceled = 0 AND f2.canceled = 0" +
                  "ORDER BY total_time, f1.fid, f2.fid";

  private PreparedStatement flightCapacityStmt;
  private PreparedStatement matchingUsernameStmt;
  private PreparedStatement addUserStmt;
  private PreparedStatement findDirectItineraryStmt;
  private PreparedStatement findIndirectItineraryStmt;

  //
  // Instance variables
  //

  private boolean loggedIn;
  private PasswordUtils managePassword;

  protected Query() throws SQLException, IOException {
    prepareStatements();
    loggedIn = false;
    managePassword = new PasswordUtils();
  }

  /**
   * Clear the data in any custom tables created.
   * 
   * WARNING! Do not drop any tables and do not clear the flights table.
   */
  public void clearTables() {
    try {
      Statement statement = conn.createStatement() ;

      // Clear Reservations_ckirby03
      String clear1 = "DELETE FROM Reservations_ckirby03";
      statement.executeUpdate(clear1);

      // Clear Itineraries_ckirby03
      String clear2 = "DELETE FROM Itineraries_ckirby03";
      statement.executeUpdate(clear2);

      // Clear Users_ckirby03
      String clear3 = "DELETE FROM Users_ckirby03";
      statement.executeUpdate(clear3);

    } catch (SQLException e) {
        e.printStackTrace();
    }
  }

  /*
   * prepare all the SQL statements in this method.
   */
  private void prepareStatements() throws SQLException {
    flightCapacityStmt = conn.prepareStatement(FLIGHT_CAPACITY_SQL);
    matchingUsernameStmt = conn.prepareStatement(MATCHING_USERNAME_SQL);
    addUserStmt = conn.prepareStatement(ADD_USER_SQL);
    findDirectItineraryStmt = conn.prepareStatement(FIND_DIRECT_ITINERARY_SQL);
    findIndirectItineraryStmt = conn.prepareStatement(FIND_INDIRECT_ITINERARY_SQL);

  }

  /**
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username user's username
   * @param password user's password
   *
   * @return If someone has already logged in, then return "User already logged in\n".  For all
   *         other errors, return "Login failed\n". Otherwise, return "Logged in as [username]\n".
   */
  public String transaction_login(String username, String password) {
    if (loggedIn) {
      return "User already logged in\n";
    }
    try {
      if (matchingLogin(username, password)) {
        loggedIn = true;
        return "Logged in as " + username + "\n";
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return "Login failed\n";
  }

  /**
   * Implement the create user function.
   *
   * @param username   new user's username. User names are unique the system.
   * @param password   new user's password.
   * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure
   *                   otherwise).
   *
   * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
   */
  public String transaction_createCustomer(String username, String password, int initAmount) {
    if (initAmount < 0) {
      return "Failed to create user\n";
    }
    boolean usernameExists = true;
    try {
      usernameExists = matchingUsername(username);
    } catch (SQLException e){
      e.printStackTrace();
    }

    if (usernameExists) {
      return "Failed to create user\n";
    } else {
      try {
        addUser(username, password, initAmount);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return "Created user " + username + "\n";
  }

  // Returns true if parameterized username exists in the database (case-insensitively)
  private boolean matchingUsername(String username) throws SQLException {
    matchingUsernameStmt.clearParameters();
    matchingUsernameStmt.setString(1, username);
    ResultSet results = matchingUsernameStmt.executeQuery();
    int numEquivalentUsernames = 0;
    while (results.next()) {
      numEquivalentUsernames++;
    }
    results.close();
    return numEquivalentUsernames != 0;
  }

  // Returns true if parameterized username and password matches a registered
  //    username/password combination
  private boolean matchingLogin(String username, String password) throws SQLException {
    matchingUsernameStmt.clearParameters();
    matchingUsernameStmt.setString(1, username);
    ResultSet results = matchingUsernameStmt.executeQuery();
    while (results.next()) {
      byte[] storedHash = results.getBytes("password");
      results.close();
      return managePassword.plaintextMatchesSaltedHash(password, storedHash);
    }
    return false;
  }

  // Adds user to database
  private void addUser(String username, String password, int initAmount) throws SQLException {
    addUserStmt.clearParameters();
    addUserStmt.setString(1, username);
    addUserStmt.setBytes(2, managePassword.saltAndHashPassword(password));
    addUserStmt.setInt(3, initAmount);
    addUserStmt.executeUpdate();
  }

  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination city, on the given
   * day of the month. If {@code directFlight} is true, it only searches for direct flights,
   * otherwise is searches for direct flights and flights with two "hops." Only searches for up
   * to the number of itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight        if true, then only search for direct flights, otherwise include
   *                            indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return, must be positive
   *
   * @return If no itineraries were found, return "No flights match your selection\n". If an error
   *         occurs, then return "Failed to search\n".
   *
   *         Otherwise, the sorted itineraries printed in the following format:
   *
   *         Itinerary [itinerary number]: [number of flights] flight(s), [total flight time]
   *         minutes\n [first flight in itinerary]\n ... [last flight in itinerary]\n
   *
   *         Each flight should be printed using the same format as in the {@code Flight} class.
   *         Itinerary numbers in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, 
                                   boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries) {

    if (numberOfItineraries <= 0) {
      return "Failed to search\n";
    }
    StringBuffer sb = new StringBuffer();
    try {
      sb = findItineraries(sb, originCity, destinationCity,
              dayOfMonth, numberOfItineraries, directFlight);
      if (sb.length() == 0) {
        return "No flights match your selection\n";
      } else {
        return sb.toString();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return "Failed to search\n";
  }

  private StringBuffer findItineraries(StringBuffer sb, String originCity, String destCity,
        int dayOfMonth, int numItineraries, boolean directFlight) throws SQLException {
    findDirectItineraryStmt.clearParameters();
    findDirectItineraryStmt.setString(1, originCity);
    findDirectItineraryStmt.setString(2, destCity);
    findDirectItineraryStmt.setInt(3, dayOfMonth);
    ResultSet results = findDirectItineraryStmt.executeQuery();
    int numProcessed = 0;
    while (results.next() && numProcessed < numItineraries) {
      sb.append("Itinerary " + numProcessed + ": 1 flight(s), "
                      + results.getInt("actual_time") + " minutes \n"
              + "ID: " + results.getInt("fid")
              + " Day: " + results.getInt("day_of_month")
              + " Carrier: " + results.getString("carrier_id")
              + " Number: " + results.getInt("flight_num")
              + " Origin: " + results.getString("origin_city")
              + " Destination: " + results.getString("dest_city")
              + " Duration: " + results.getInt("actual_time")
              + " Capacity: " + results.getInt("capacity")
              + " Price: " + results.getInt("price") + "\n");
      numProcessed++;
    }
    results.close();

    if (!directFlight && numProcessed < numItineraries) {
      findIndirectItineraryStmt.clearParameters();
      findIndirectItineraryStmt.setString(1, originCity);
      findIndirectItineraryStmt.setString(2, destCity);
      findIndirectItineraryStmt.setInt(3, dayOfMonth);
      ResultSet results2 = findIndirectItineraryStmt.executeQuery();
      while (results2.next() && numProcessed < numItineraries) {
        sb.append("Itinerary " + numProcessed + ": 2 flight(s), "
                + results2.getInt("total_time") + " minutes \n"
                + "ID: " + results2.getInt("f1_fid")
                + " Day: " + results2.getInt("f1_day_of_month")
                + " Carrier: " + results2.getString("f1_carrier_id")
                + " Number: " + results2.getInt("f1_flight_num")
                + " Origin: " + results2.getString("f1_origin_city")
                + " Destination: " + results2.getString("f1_dest_city")
                + " Duration: " + results2.getInt("f1_actual_time")
                + " Capacity: " + results2.getInt("f1_capacity")
                + " Price: " + results2.getInt("f1_price") + "\n"
                + "ID: " + results2.getInt("f2_fid")
                + " Day: " + results2.getInt("f2_day_of_month")
                + " Carrier: " + results2.getString("f2_carrier_id")
                + " Number: " + results2.getInt("f2_flight_num")
                + " Origin: " + results2.getString("f2_origin_city")
                + " Destination: " + results2.getString("f2_dest_city")
                + " Duration: " + results2.getInt("f2_actual_time")
                + " Capacity: " + results2.getInt("f2_capacity")
                + " Price: " + results2.getInt("f2_price") + "\n");
        numProcessed++;
      }
      results2.close();
    }
    return sb;
  }

  /**
   * Implements the book itinerary function.
   *
   * @param itineraryId ID of the itinerary to book. This must be one that is returned by search
   *                    in the current session.
   *
   * @return If the user is not logged in, then return "Cannot book reservations, not logged
   *         in\n". If the user is trying to book an itinerary with an invalid ID or without
   *         having done a search, then return "No such itinerary {@code itineraryId}\n". If the
   *         user already has a reservation on the same day as the one that they are trying to
   *         book now, then return "You cannot book two flights in the same day\n". For all
   *         other errors, return "Booking failed\n".
   *
   *         If booking succeeds, return "Booked flight(s), reservation ID: [reservationId]\n"
   *         where reservationId is a unique number in the reservation system that starts from
   *         1 and increments by 1 each time a successful reservation is made by any user in
   *         the system.
   */
  public String transaction_book(int itineraryId) {
    // TODO: YOUR CODE HERE
    return "Booking failed\n";
  }

  /**
   * Implements the pay function.
   *
   * @param reservationId the reservation to pay for.
   *
   * @return If no user has logged in, then return "Cannot pay, not logged in\n". If the
   *         reservation is not found / not under the logged in user's name, then return
   *         "Cannot find unpaid reservation [reservationId] under user: [username]\n".  If
   *         the user does not have enough money in their account, then return
   *         "User has only [balance] in account but itinerary costs [cost]\n".  For all other
   *         errors, return "Failed to pay for reservation [reservationId]\n"
   *
   *         If successful, return "Paid reservation: [reservationId] remaining balance:
   *         [balance]\n" where [balance] is the remaining balance in the user's account.
   */
  public String transaction_pay(int reservationId) {
    // TODO: YOUR CODE HERE
    return "Failed to pay for reservation " + reservationId + "\n";
  }

  /**
   * Implements the reservations function.
   *
   * @return If no user has logged in, then return "Cannot view reservations, not logged in\n" If
   *         the user has no reservations, then return "No reservations found\n" For all other
   *         errors, return "Failed to retrieve reservations\n"
   *
   *         Otherwise return the reservations in the following format:
   *
   *         Reservation [reservation ID] paid: [true or false]:\n [flight 1 under the
   *         reservation]\n [flight 2 under the reservation]\n Reservation [reservation ID] paid:
   *         [true or false]:\n [flight 1 under the reservation]\n [flight 2 under the
   *         reservation]\n ...
   *
   *         Each flight should be printed using the same format as in the {@code Flight} class.
   *
   * @see Flight#toString()
   */
  public String transaction_reservations() {
    // TODO: YOUR CODE HERE
    return "Failed to retrieve reservations\n";
  }

  /**
   * Example utility function that uses prepared statements
   */
  private int checkFlightCapacity(int fid) throws SQLException {
    flightCapacityStmt.clearParameters();
    flightCapacityStmt.setInt(1, fid);

    ResultSet results = flightCapacityStmt.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }

  /**
   * Utility function to determine whether an error was caused by a deadlock
   */
  private static boolean isDeadlock(SQLException e) {
    return e.getErrorCode() == 1205;
  }

  /**
   * A class to store information about a single flight
   */
  class Flight {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    Flight(int id, int day, String carrier, String fnum, String origin, String dest, int tm,
           int cap, int pri) {
      fid = id;
      dayOfMonth = day;
      carrierId = carrier;
      flightNum = fnum;
      originCity = origin;
      destCity = dest;
      time = tm;
      capacity = cap;
      price = pri;
    }
    
    @Override
    public String toString() {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId + " Number: "
          + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time
          + " Capacity: " + capacity + " Price: " + price;
    }
  }
}
