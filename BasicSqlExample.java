package BasicSql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Main example application class.
 */
public class BasicSqlExample {

    /**
     * A simple sql api example, to demonstrate accessing a table (or view) in the local system.
     *
     * Note that we assume that the database examples exists, with the schema scompany, and table
     * or view emp_address. Also, we assume that the role User123_4 exists, with appropriately granted
     * access to the table or view. Also, the appropriate external library jdbc driver library is needed
     * (e.g. postgresql-42.2.12.jar).
     *
     * @param args Command-line arguments (not used).
     */
    public static void main(String[] args) {

        final String url = "jdbc:postgresql://localhost/21sessions";
        final String user = "User123";
        // Never a good idea to store passowrds uncrypted, use e.g. java.io.Console instead.
        final String password = "pw123";

        // Try opening a connection. Might fail, e.g., due to missing access rights.
        Connection connection = null;
        try {
            // Get connection.
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            // Did not work; this is a fatal error so quit.
            System.out.println("Fatal error: " + e.getMessage());
            System.exit(1);
        }

        // Collect and construct a string result, to be printed.
        String sql = "SELECT * from taxi_company.person";
        StringBuilder result = new StringBuilder();
        try {
            // Interpret and execute the input as an sql query statement.
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            ResultSetMetaData resultSetMetadata = resultSet.getMetaData();
            int columns = resultSetMetadata.getColumnCount();
            int rows = 0;

            // Iterate all results.
            while (resultSet.next()) {

                // Print column names, if any.
                if(resultSet.isFirst() && columns > 0) {
                    result.append("| ");
                    for(int i = 1; i <= columns; ++i) {
                        result.append(resultSetMetadata.getColumnLabel(i)).append(" | ");
                    }
                    result.append("\r\n");
                }

                // Print all column values as a list.
                result.append("( ");
                // Note that here indices start from 1 (but in Java indices start from 0).
                for (int i = 1; i <= columns; ++i) {
                    String value = resultSet.getString(i);
                    result.append(value);
                    if(i < columns) result.append(", ");
                }
                result.append(" )\r\n");
                ++rows;
            }

            result.append("Total ").append(rows).append(" records(s) found.\r\n");
            resultSet.close();
            statement.close();

        } catch (SQLException e) {
            result.append("Error: ").append(e.getMessage()).append("\r\n");
        }

        // Print the result.
        System.out.println(result);

        // Close the connection.
        try {
            connection.close();
        } catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }
    }
}
