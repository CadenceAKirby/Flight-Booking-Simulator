# Flight Booking Simulator

Java CLI application that simulates a flight reservation system: users can search itineraries, book seats, and pay for reservations against a SQL Server backend. Built for the CSE344 databases course as a hands-on project in transactions and JDBC.

## Features
- Command-driven REPL with `login`, `create`, `search`, `book`, `reservations`, and `pay`
- Transaction-safe operations via JDBC with configurable SQL Server connection
- Password utilities with salted PBKDF2 hashing
- Database bootstrap script for user, itinerary, and reservation tables
- JUnit test suite covering the booking workflow and password helpers

## Tech Stack
- Java 11
- Maven
- Microsoft SQL Server (JDBC)
- JUnit 4

## Prerequisites
- JDK 11+
- Maven
- Access to a SQL Server instance with the base tables (FLIGHTS, CARRIERS, MONTHS, WEEKDAYS)

## Setup
1. Copy `dbconn.properties` from the course starter or fill in the values:
   - `flightapp.server_url`
   - `flightapp.database_name`
   - `flightapp.username`
   - `flightapp.password`
   - Optional: `flightapp.tablename_suffix` if you need isolated table names
2. Create the project tables on your SQL Server using `createTables.sql` (adjust the suffix if required).
3. Confirm connectivity: `mvn -q exec:java -Dexec.mainClass=flightapp.FlightService -DskipTests`

## Running the CLI
```bash
mvn exec:java -Dexec.mainClass=flightapp.FlightService
```
You will be dropped into an interactive prompt. Common commands:
- `create <username> <password> <initial_balance>`
- `login <username> <password>`
- `search <origin_city> <destination_city> <direct 0|1> <day_of_month> <max_itineraries>`
- `book <itinerary_id>`
- `reservations`
- `pay <reservation_id>`
- `quit`

## Testing
```bash
mvn test
```
The suite in `src/test/java/flightapp` uses cases under `cases/` and the bundled `distributed.jar` to validate query behavior and password hashing.

## Project Structure
- `src/main/java/flightapp` — core CLI, query logic, DB/password utilities
- `src/test/java/flightapp` — JUnit tests and helpers
- `createTables.sql` — schema for per-user tables
- `pom.xml` — Maven build configuration

## Notes
- Transactions default to `SERIALIZABLE` isolation; adjust in `DBConnUtils` if your environment requires different settings.
- The app assumes the base flight data tables already exist; do not modify them in your implementation.
