import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class jbdc {

    public static void main(String[] args) {
        // Prints "Hello, World" to the terminal window.
        String url = "jdbc:mysql://localhost:3306/wikistats";
        String username = "wikistats";
        //change this if you want to connect sucessfully
        String password = "passwd";

        System.out.println("Connecting database...");
        System.out.println(username);
        System.out.println(password);
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            System.out.println("Database connected!");
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot connect the database!", e);
        }
    }

}
