import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;

public class Test_Index {
    private static final int batch_size = 5000;
    private static Connection con = null;
    private static PreparedStatement insert = null;
    private static PreparedStatement delete = null;
    private static PreparedStatement select_i = null;
    private static PreparedStatement select_c = null;
//    private static PreparedStatement client = null;
//    private static PreparedStatement supplycenter = null;
    private static boolean verbose = false;

    private static void openDB(String host, String dbname, String user, String password) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (Exception e) {
            System.err.println("Cannot find the Postgres driver. Check ClassPath");
            System.exit(1);
        }
        String url = "jdbc:postgresql://" + host + "/" + dbname;
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        try {
            con = DriverManager.getConnection(url, props);
            if (verbose) {
                System.out.println("Successfully connected to the database "
                        + dbname + " as " + user);
            }
            con.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }

        try {
            insert = con.prepareStatement("insert into \"order\"(order_id,quantity,lodgement_date,estimated_delivery,salesman_number,model_id,contract_id)"
                    + " values(?,?,?,?,?,?,?)");
            delete = con.prepareStatement("delete from \"order\" where order_id = ?");
            select_i = con.prepareStatement("select quantity,lodgement_date from \"order\" where order_id = ?");
            select_c = con.prepareStatement("select quantity,lodgement_date from \"order\" where contract_id = ?");

        } catch (SQLException e) {
            System.err.println("Insert statement failed");
            System.err.println(e.getMessage());
            closeDB();
            System.exit(1);
        }

    }

    private static void closeDB() {
        if (con != null) {
            try {
                if (insert != null) {
                    insert.close();
                }
                if (delete != null) {
                    delete.close();
                }
                if (select_i != null) {
                    select_i.close();
                }
                if (select_c != null) {
                    select_c.close();
                }
                con.close();
                con = null;
            } catch (Exception e) {
                //System.err.println("Fatal error: " + e.getMessage());
            }
        }
    }

    static int orderid=1;
    private static void insert_order(int quantity, String l_date, String e_date, int s_number, int m_name, int c_num) {
        try {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            Date l = df.parse(l_date);
            Date e = df.parse(e_date);
            java.sql.Date sqlf = new java.sql.Date(l.getTime());
            java.sql.Date sqle = new java.sql.Date(e.getTime());
            insert.setInt(1,orderid++);
            insert.setInt(2,quantity);
            insert.setDate(3,sqlf);
            insert.setDate(4,sqle);
            insert.setInt(5,s_number);
            insert.setInt(6,m_name);
            insert.setInt(7,c_num);
            insert.addBatch();
        }catch (SQLException | ParseException throwables) {
            //System.err.println("Fatal error: " + e.getMessage());
        }
    }

    private static void delete_order(int id) {
        try {
            delete.setInt(1,id);
            delete.addBatch();
        }catch (SQLException throwables) {
            //
        }
    }

    private static void select_id(int order_id) {
        try {
            select_i.setInt(1,order_id);
        }catch (SQLException throwables) {
            //
        }
    }

    private static void select_order(int contract_id) {
        try {
            select_c.setInt(1,contract_id);
        }catch (SQLException throwables) {
            //
        }
    }

    public static void main(String [] args) {
        String order_table = Utils.join("tables","order.csv");
        String insert_orders = Utils.join("tables","insert_data.csv"); // 20,000 orders.

        Properties defprop = new Properties();
        defprop.put("host", "localhost");
        defprop.put("user", "leolu");
        defprop.put("password", "");
        defprop.put("database", "project");
        Properties prop = new Properties(defprop);

        /*
            init the order table.
         */
        int count = 0;
        try (BufferedReader infile = new BufferedReader(new FileReader(order_table))) {
            long start, end;
            String line;
            String[] data;

            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"), prop.getProperty("user"), prop.getProperty("password"));

            infile.readLine();
            while ((line = infile.readLine()) != null) {
                data = line.split(",");
                insert_order(Integer.parseInt(data[1]),data[2],data[3],Integer.parseInt(data[4]),
                        Integer.parseInt(data[5]),Integer.parseInt(data[6]));

                count++;
                if (count % batch_size == 0) {
                    insert.executeBatch();
                }
            }
            if (count % batch_size != 0) {
                insert.executeBatch();
            }

            con.commit();
            insert.close();
            closeDB();

            end = System.currentTimeMillis();
            System.out.println(count + " records successfully loaded");
            System.out.println("Loading speed : "
                    + (count * 1000L) / (end - start)
                    + " records/s");

        }  catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
                insert.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                insert.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }


        /*
            insert 20,000 orders.
         */
        try (BufferedReader infile = new BufferedReader(new FileReader(insert_orders))) {
            long start, end;
            String line;
            String[] data;
            count = 0;


            openDB(prop.getProperty("host"), prop.getProperty("database"), prop.getProperty("user"), prop.getProperty("password"));

            start = System.currentTimeMillis();
            while ((line = infile.readLine()) != null) {
                data = line.split(",");
                insert_order(Integer.parseInt(data[1]),data[2],data[3],Integer.parseInt(data[4]),
                        Integer.parseInt(data[5]),Integer.parseInt(data[6]));
                count++;
                if (count % batch_size == 0) {
                    insert.executeBatch();
                }
            }
            if (count % batch_size != 0) {
                insert.executeBatch();
            }
            end = System.currentTimeMillis();

            con.commit();
            insert.close();
            closeDB();

            System.out.printf("%d orders successfully inserted. It costs %d ms.\n",count,(end-start));

        }  catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
                insert.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                insert.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }
        closeDB();


        /*
            select_c test.
         */
        try {
            long start, end;

            openDB(prop.getProperty("host"), prop.getProperty("database"), prop.getProperty("user"), prop.getProperty("password"));
            count = 0;

            start = System.currentTimeMillis();
            for (int i = 0; i < 2000; i++) {
                select_order(i+1);
                count++;
                select_c.executeQuery();
            }
            end = System.currentTimeMillis();

            con.commit();
            select_c.close();
            closeDB();

            System.out.printf("%d selects by contract_id. It costs %d ms.\n",count,(end-start));

        }  catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
                select_c.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }
        closeDB();

    /*
        select_i test.
     */
    try {
        long start, end;
        count = 0;


        openDB(prop.getProperty("host"), prop.getProperty("database"), prop.getProperty("user"), prop.getProperty("password"));

        start = System.currentTimeMillis();
        for (int i = 0; i < 2000; i++) {
            select_id(i+1);
            count++;
            select_i.executeQuery();
        }
        end = System.currentTimeMillis();

        con.commit();
        select_i.close();
        closeDB();


        System.out.printf("%d orders selects by order_id. It costs %d ms.\n",count,(end-start));

    }  catch (SQLException se) {
        System.err.println("SQL error: " + se.getMessage());
        try {
            con.rollback();
            select_i.close();
        } catch (Exception e2) {
        }
        closeDB();
        System.exit(1);
    }
    closeDB();
    }
}
