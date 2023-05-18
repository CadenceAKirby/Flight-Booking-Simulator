package flightapp;

import javax.xml.transform.Result;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

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
                  "f1.actual_time + f2.actual_time AS total_time " +
                  "FROM FLIGHTS AS f1 " +
                  "JOIN FLIGHTS AS f2 ON f1.dest_city = f2.origin_city " +
                  "WHERE f1.origin_city = ? AND f2.dest_city = ? AND f1.day_of_month = ? AND f2.day_of_month = ? " +
                  "AND f1.canceled = 0 AND f2.canceled = 0 " +
                  "ORDER BY total_time, f1.fid, f2.fid";
  private static final String FIND_DAY_AND_MONTH_FROM_ID_SQL = "SELECT day_of_month, month_id FROM FLIGHTS WHERE fid = ?";

  private static final String ADD_ITINERARY_SQL = "INSERT INTO Itineraries_ckirby03 VALUES(?, ?, ?)";

  private static final String RETRIEVE_UNPAID_RESERVATION_SQL =
          "SELECT * FROM Reservations_ckirby03 WHERE rid = ? AND paid = 0";

  private static final String NUM_ITINERARIES_BY_DAY_USER_SQL =
          "SELECT *"
        + " FROM Reservations_ckirby03 r"
        + " INNER JOIN Itineraries_ckirby03 i ON r.itid = i.itid"
        + " INNER JOIN FLIGHTS f ON i.fid1 = f.fid OR i.fid2 = f.fid"
        + " WHERE r.username = ?"
        + " AND (f.month_id = ?"
        + " AND f.day_of_month = ?)";
  private static final String MAKE_NEW_RESERVATION_SQL =
          "INSERT INTO Reservations_ckirby03 VALUES (?, ?, 0, ?)";

  private static final String RETRIEVE_BALANCE_SQL =
          "SELECT balance FROM Users_ckirby03 WHERE username = ?";

  private static final String UPDATE_BALANCE_SQL =
          "UPDATE Users_ckirby03 SET balance = ? - ? WHERE username = ?";

  private static final String RETRIEVE_USER_RESERVATION_INFO_SQL =
          "SELECT r.rid, r.paid, f1.fid AS f1_fid, f1.day_of_month AS f1_day_of_month, f1.carrier_id AS f1_carrier_id," +
          " f1.flight_num AS f1_flight_num, f1.origin_city AS f1_origin_city," +
          "f1.dest_city AS f1_dest_city, f1.actual_time AS f1_actual_time, f1.capacity AS f1_capacity," +
          "f1.price AS f1_price, f2.fid AS f2_fid, f2.day_of_month AS f2_day_of_month," +
          "f2.carrier_id AS f2_carrier_id, f2.flight_num AS f2_flight_num, f2.origin_city AS f2_origin_city," +
          "f2.dest_city AS f2_dest_city, f2.actual_time AS f2_actual_time, f2.capacity AS f2_capacity," +
          "f2.price AS f2_price " +
          "FROM Reservations_ckirby03 AS r " +
          "JOIN Itineraries_ckirby03 AS i ON i.itid = r.itid " +
          "LEFT OUTER JOIN FLIGHTS AS f1 ON i.fid1 = f1.fid " +
          "LEFT OUTER JOIN FLIGHTS AS f2 ON i.fid2 = f2.fid " +
          "WHERE r.username = ?";




  private PreparedStatement flightCapacityStmt;
  private PreparedStatement matchingUsernameStmt;
  private PreparedStatement addUserStmt;
  private PreparedStatement findDirectItineraryStmt;
  private PreparedStatement findIndirectItineraryStmt;
  private PreparedStatement findDayAndMonthFromIdStmt;
  private PreparedStatement addItineraryStmt;
  private PreparedStatement retrieveUnpaidReservationStmt;
  private PreparedStatement numItinerariesByDayUserStmt;
  private PreparedStatement makeNewReservationStmt;
  private PreparedStatement updateBalanceStmt;
  private PreparedStatement retrieveBalanceStmt;
  private PreparedStatement retrieveUserReservationInfoStmt;

  // Instance variables
  private boolean loggedIn;
  private PasswordUtils managePassword;
  private HashMap<Integer, Flight[]> providedItineraries;
  private String user;

  protected Query() throws SQLException, IOException {
    prepareStatements();
    loggedIn = false;
    managePassword = new PasswordUtils();
    providedItineraries = new HashMap<>();
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
    findDayAndMonthFromIdStmt = conn.prepareStatement(FIND_DAY_AND_MONTH_FROM_ID_SQL);
    addItineraryStmt = conn.prepareStatement(ADD_ITINERARY_SQL);
    retrieveUnpaidReservationStmt = conn.prepareStatement(RETRIEVE_UNPAID_RESERVATION_SQL);
    numItinerariesByDayUserStmt = conn.prepareStatement(NUM_ITINERARIES_BY_DAY_USER_SQL);
    makeNewReservationStmt = conn.prepareStatement(MAKE_NEW_RESERVATION_SQL);
    updateBalanceStmt = conn.prepareStatement(UPDATE_BALANCE_SQL);
    retrieveBalanceStmt = conn.prepareStatement(RETRIEVE_BALANCE_SQL);
    retrieveUserReservationInfoStmt = conn.prepareStatement(RETRIEVE_USER_RESERVATION_INFO_SQL);
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
        user = username.toLowerCase();
        return "Logged in as " + user + "\n";
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
    } catch (SQLException e) {
      e.printStackTrace();
        try {
          conn.rollback();
        } catch (SQLException a) {
          a.printStackTrace();
        }
    }

    if (usernameExists) {
      return "Failed to create user\n";
    } else {
      try {
        addUser(username, password, initAmount);
      } catch (SQLException e) {
          try {
            conn.rollback();
            conn.setAutoCommit(true);
          } catch (SQLException a) {
            a.printStackTrace();
          }
      }
    }
    return "Created user " + username + "\n";
  }

  // Returns true if parameterized username exists in the database (case-insensitively)
  private boolean matchingUsername(String username) throws SQLException {
    matchingUsernameStmt.clearParameters();
    matchingUsernameStmt.setString(1, username);
    conn.setAutoCommit(false);
    ResultSet results = matchingUsernameStmt.executeQuery();
    int numEquivalentUsernames = 0;
    while (results.next()) {
      numEquivalentUsernames++;
    }
    results.close();
    conn.commit();
    conn.setAutoCommit(true);
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
    conn.setAutoCommit(false);
    addUserStmt.executeUpdate();
    conn.commit();
    conn.setAutoCommit(true);
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
    try {
      StringBuffer sb = findItineraries(originCity, destinationCity,
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

  private StringBuffer findItineraries(String originCity, String destCity,
        int dayOfMonth, int numItineraries, boolean directFlight) throws SQLException {
    ArrayList<Compact> itineraries = new ArrayList<>();
    providedItineraries.clear();
    findDirectItineraryStmt.clearParameters();
    findDirectItineraryStmt.setString(1, originCity);
    findDirectItineraryStmt.setString(2, destCity);
    findDirectItineraryStmt.setInt(3, dayOfMonth);
    ResultSet results = findDirectItineraryStmt.executeQuery();
    int numProcessed = 0;
    while (results.next() && numProcessed < numItineraries) {
      int itineraryId = results.getInt("fid");
      Flight flight = new Flight(itineraryId, results.getInt("day_of_month"),
              results.getString("carrier_id"), results.getInt("flight_num") + "",
              results.getString("origin_city"), results.getString("dest_city"),
              results.getInt("actual_time"), results.getInt("capacity"),
              results.getInt("price"));
      Flight[] flights = {flight, null};
      numProcessed++;
      itineraries.add(new Compact(flights, results.getInt("actual_time")));
    }
    results.close();

    if (!directFlight && numProcessed < numItineraries) {
      findIndirectItineraryStmt.clearParameters();
      findIndirectItineraryStmt.setString(1, originCity);
      findIndirectItineraryStmt.setString(2, destCity);
      findIndirectItineraryStmt.setInt(3, dayOfMonth);
      findIndirectItineraryStmt.setInt(4, dayOfMonth);
      ResultSet results2 = findIndirectItineraryStmt.executeQuery();
      while (results2.next() && numProcessed < numItineraries) {
        Flight flight1 = new Flight(results2.getInt("f1_fid"), results2.getInt("f1_day_of_month"),
                results2.getString("f1_carrier_id"), results2.getInt("f1_flight_num") + "",
                results2.getString("f1_origin_city"), results2.getString("f1_dest_city"),
                results2.getInt("f1_actual_time"), results2.getInt("f1_capacity"),
                results2.getInt("f1_price"));
        Flight flight2 = new Flight(results2.getInt("f2_fid"), results2.getInt("f2_day_of_month"),
                results2.getString("f2_carrier_id"), results2.getInt("f2_flight_num") + "",
                results2.getString("f2_origin_city"), results2.getString("f2_dest_city"),
                results2.getInt("f2_actual_time"), results2.getInt("f2_capacity"),
                results2.getInt("f2_price"));
        Flight[] flights = {flight1, flight2};
        numProcessed++;
        itineraries.add(new Compact(flights, results2.getInt("total_time")));
      }
      results2.close();
    }
    StringBuffer sb = new StringBuffer();
    numProcessed = 0;
    Collections.sort(itineraries, new CompactComparator());
    for (Compact itinerary : itineraries) {
      if (itinerary.flights[1] == null) {
        sb.append("Itinerary " + numProcessed + ": 1 flight(s), "
                + itinerary.duration + " minutes\n"
                + itinerary.flights[0] + "\n");
      } else {
        sb.append("Itinerary " + numProcessed + ": 2 flight(s), "
                + itinerary.duration + " minutes\n"
                + itinerary.flights[0] + "\n" + itinerary.flights[1] + "\n");
      }
      providedItineraries.put(numProcessed, itinerary.flights);
      numProcessed++;
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
    if (!loggedIn) {
      return "Cannot book reservations, not logged in\n";
    }
    if (!providedItineraries.containsKey(itineraryId)) {
      return "No such itinerary {@code itineraryId}\n";
    }
    int itid = 1;
    try {
      Statement statement = conn.createStatement();
      String query = "SELECT itid FROM Itineraries_ckirby03 WHERE fid1 = "
              + providedItineraries.get(itineraryId)[0].fid;
      if (providedItineraries.get(itineraryId)[1] != null) {
        query += "AND fid2 = " + providedItineraries.get(itineraryId)[1].fid;
      }
      ResultSet results = statement.executeQuery(query);
      if (results.next()) {
        itid = results.getInt("itid");
      }
      results.close();
      findDayAndMonthFromIdStmt.setInt(1, providedItineraries.get(itineraryId)[0].fid);
      ResultSet results2 = findDayAndMonthFromIdStmt.executeQuery();
      results2.next();
      int month = results2.getInt("month_id");
      int day = results2.getInt("day_of_month");
      results2.close();
      numItinerariesByDayUserStmt.setString(1, user);
      numItinerariesByDayUserStmt.setInt(2, month);
      numItinerariesByDayUserStmt.setInt(3, day);
      ResultSet results3 = numItinerariesByDayUserStmt.executeQuery();
      if (results3.next()) {
        return "You cannot book two flights in the same day\n";
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    try {
      Statement statement = conn.createStatement();
      String query2 = "SELECT MAX(itid) FROM Itineraries_ckirby03";
      ResultSet results2 = statement.executeQuery(query2);
      if (results2.next()) {
        itid = results2.getInt(1) + 1;
      }

      addItineraryStmt.setInt(1, itid);
      addItineraryStmt.setInt(2, providedItineraries.get(itineraryId)[0].fid);
      if (providedItineraries.get(itineraryId)[1] == null) {
        addItineraryStmt.setNull(3, java.sql.Types.INTEGER);
      } else {
        addItineraryStmt.setInt(3, providedItineraries.get(itineraryId)[1].fid);
      }
      addItineraryStmt.executeUpdate();
      String query3 = "SELECT MAX(rid) FROM Reservations_ckirby03";
      ResultSet results3 = statement.executeQuery(query3);
      int rid = 1;
      if (results3.next()) {
        rid = results3.getInt(1) + 1;
      }

      makeNewReservationStmt.setInt(1, rid);
      makeNewReservationStmt.setString(2, user);
      makeNewReservationStmt.setInt(3, itid);
      makeNewReservationStmt.executeUpdate();
      return "Booked flight(s), reservation ID: " + rid + "\n";

    } catch (SQLException e){
      e.printStackTrace();
    }
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
    if (!loggedIn) {
      return "Cannot pay, not logged in\n";
    }
    try {
      retrieveUnpaidReservationStmt.setInt(1, reservationId);
      ResultSet results = retrieveUnpaidReservationStmt.executeQuery();
      if (!results.next()) {
        results.close();
        return "Cannot find unpaid reservation " + reservationId + " under user: " + user + "\n";
      }
      if (!results.getString("username").equalsIgnoreCase(user)) {
        results.close();
        return "Cannot find unpaid reservation " + reservationId + " under user: " + user + "\n";
      }
      int itineraryId = results.getInt("itid");
      Statement statement = conn.createStatement();
      String query = "SELECT fid1, fid2 FROM Itineraries_ckirby03 AS i WHERE i.itid = " + itineraryId;
      ResultSet results2 = statement.executeQuery(query);
      results2.next();
      int flightId1 = results2.getInt("fid1");
      int flightId2 = results2.getInt("fid2");
      results2.close();
      int price = flightPrice(flightId1);
      if (flightId2 != 0) {
        price += flightPrice(flightId2);
      }
      retrieveBalanceStmt.setString(1, user);
      ResultSet results3 = retrieveBalanceStmt.executeQuery();
      results3.next();
      int balance = results3.getInt("balance");
      results3.close();
      if (balance < price) {
        return "User has only " + balance + " in account but itinerary costs " + price + "\n";
      }
      updateBalanceStmt.setInt(1, balance);
      updateBalanceStmt.setInt(2, price);
      updateBalanceStmt.setString(3, user);
      updateBalanceStmt.executeUpdate();
      String query2 = "UPDATE Reservations_ckirby03 SET paid = 1";
      statement.executeUpdate(query2);
      return "Paid reservation: " + reservationId + " remaining balance: " + (balance - price) + "\n";
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return "Failed to pay for reservation " + reservationId + "\n";
  }

  // Returns price of flight having the parameterized flight ID
  private int flightPrice(int flightId) throws SQLException {
    Statement statement = conn.createStatement();
    String query = "SELECT price FROM FLIGHTS WHERE fid = " + flightId;
    ResultSet results = statement.executeQuery(query);
    results.next();
    int price = results.getInt("price");
    results.close();
    return price;
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
    if (!loggedIn) {
      return "Cannot view reservations, not logged in\n";
    }
    try {
      retrieveUserReservationInfoStmt.setString(1, user);
      ResultSet results = retrieveUserReservationInfoStmt.executeQuery();
      StringBuffer sb = new StringBuffer();
      if (!results.next()) {
        return "No reservations found\n";
      } else {
        sb = outputReservations(sb, results);
        while (results.next()) {
          sb = outputReservations(sb, results);
        }
      }
      results.close();
      return sb.toString();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return "Failed to retrieve reservations\n";
  }

  private StringBuffer outputReservations(StringBuffer sb, ResultSet results) throws SQLException {
    boolean paid = (results.getInt("paid") == 1);
    sb.append("Reservation " + results.getInt("rid")+ " paid: " + paid + ":\n" +
            "ID: " + results.getInt("f1_fid") +
            " Day: " + results.getInt("f1_day_of_month") +
            " Carrier: " + results.getString("f1_carrier_id") +
            " Number: " + results.getInt("f1_flight_num") +
            " Origin: " + results.getString("f1_origin_city") +
            " Dest: " + results.getString("f1_dest_city") +
            " Duration: " + results.getInt("f1_actual_time") +
            " Capacity: " + results.getInt("f1_capacity") +
            " Price: " + results.getInt("f1_price") + "\n");
    if (results.getInt("f2_fid") != 0) {
      sb.append("ID: " + results.getInt("f2_fid") +
              " Day: " + results.getInt("f2_day_of_month") +
              " Carrier: " + results.getString("f2_carrier_id") +
              " Number: " + results.getInt("f2_flight_num") +
              " Origin: " + results.getString("f2_origin_city") +
              " Dest: " + results.getString("f2_dest_city") +
              " Duration: " + results.getInt("f2_actual_time") +
              " Capacity: " + results.getInt("f2_capacity") +
              " Price: " + results.getInt("f2_price") + "\n");
    }
    return sb;
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

  // Compactly stores a Flight[2] and the cumulative duration of all non-null flights
  //      in that array
  public class Compact {

    public Query.Flight[] flights;
    public int duration;

    public Compact(Query.Flight[] flights, int duration) {
      this.flights = flights;
      this.duration = duration;
    }
  }

  // Compact objects should be compared using duration field
  private class CompactComparator implements Comparator<Compact> {

    public int compare(Compact obj1, Compact obj2) {

      if (obj1.duration == obj2.duration) {
        return 0;
      } else if (obj1.duration < obj2.duration) {
        return -1;
      } else {
        return 1;
      }
    }
  }
}


