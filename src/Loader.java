import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.sql.*;
import java.net.URL;

public class Loader {
    private static final int batch_size = 5000;
    private static URL propertyurl = Loader.class.getResource("/loader.cnf");
    private static Connection con = null;
    private static PreparedStatement order = null;
    private static PreparedStatement product_model = null;
    private static PreparedStatement salesman = null;
    private static PreparedStatement contract = null;
    private static PreparedStatement client = null;
    private static PreparedStatement supplycenter = null;
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
            order = con.prepareStatement("insert into \"order\"(order_id,quantity,lodgement_date,estimated_delivery,salesman_number,model_id,contract_id)"
                    + " values(?,?,?,?,?,?,?)");
            product_model = con.prepareStatement("insert into product_model(model_id,model_name,product_name,product_code,price)"
                    + " values(?,?,?,?,?)");
            salesman = con.prepareStatement("insert into salesman(salesman_number,salesman_name,gender,age,phone)"
                    + " values(?,?,?,?,?)");
            contract = con.prepareStatement("insert into contract(contract_id,contract_number,contract_date,client_id)"
                    + " values(?,?,?,?)");
            client = con.prepareStatement("insert into client(client_id,enterprise,country,city,industry,supply_center_id)"
                    + " values(?,?,?,?,?,?)");
            supplycenter = con.prepareStatement("insert into supply_center(supply_center_id,supply_center_name,director)"
                    + " values(?,?,?)");
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
                if (order != null) {
                    order.close();
                }
                if (product_model != null) {
                    product_model.close();
                }
                if (salesman != null) {
                    salesman.close();
                }
                if (contract != null) {
                    contract.close();
                }
                if (client != null) {
                    client.close();
                }
                if (supplycenter != null) {
                    supplycenter.close();
                }
                con.close();
                con = null;
            } catch (Exception e) {
                //System.err.println("Fatal error: " + e.getMessage());
            }
        }
    }

    static int orderid=1;
    private static void loadorder(int quantity,String l_date,String e_date,int s_number,int m_name, int c_num) {
        try {
            DateFormat df = new SimpleDateFormat("yyyy/MM/dd");
            Date l = df.parse(l_date);
            Date e = df.parse(e_date);
            java.sql.Date sqlf = new java.sql.Date(l.getTime());
            java.sql.Date sqle = new java.sql.Date(e.getTime());
            order.setInt(1,orderid++);
            order.setInt(2,quantity);
            order.setDate(3,sqlf);
            order.setDate(4,sqle);
            order.setInt(5,s_number);
            order.setInt(6,m_name);
            order.setInt(7,c_num);
            order.addBatch();
        }catch (SQLException | ParseException throwables) {
            //System.err.println("Fatal error: " + e.getMessage());
        }
    }

    private static void loadproduct_model(int id,String name,String p_name,String p_code, int price) {
        try {
            product_model.setInt(1,id);
            product_model.setString(2,name);
            product_model.setString(3,p_name);
            product_model.setString(4,p_code);
            product_model.setInt(5,price);
            product_model.addBatch();
        }catch (SQLException throwables) {
            //
        }
    }

    private static void loadsalesman(int num,String name,String gender, int age, String phone) {
        try {
            salesman.setInt(1,num);
            salesman.setString(2,name);
            salesman.setString(3,gender);
            salesman.setInt(4,age);
            salesman.setString(5,phone);
            salesman.addBatch();
        }catch (SQLException throwables) {
            //
        }
    }

    private static void loadcontract(int id,String num, String date,int enterprise) {
        try {
            DateFormat df = new SimpleDateFormat("yyyy/MM/dd");
            Date d = df.parse(date);
            java.sql.Date sqld = new java.sql.Date(d.getTime());
            contract.setInt(1,id);
            contract.setString(2, num);
            contract.setDate(3, sqld);
            contract.setInt(4, enterprise);
            contract.addBatch();
        }catch (SQLException | ParseException throwables) {
            //
        }
    }


    private static void loadclient(int id,String enterprise,String country,String city,String industry,int sc_name) {
        try{
            client.setInt(1,id);
            client.setString(2,enterprise);
            client.setString(3,country);
            client.setString(4,city);
            client.setString(5,industry);
            client.setInt(6,sc_name);
            client.addBatch();
        } catch (SQLException throwables) {
            //
        }
    }


    private static void loadsupplycenter(int id,String sc_name, String director) {
        try {
            supplycenter.setInt(1,id);
            supplycenter.setString(2,sc_name);
            supplycenter.setString(3,director);
            supplycenter.addBatch();
        } catch (SQLException e) {
            //
        }

    }

    public static void main(String[] args) {
        String fileName = null;
        switch (args.length) {
            case 1:
                fileName = args[0];
                break;
            case 2:
                switch (args[0]) {
                    case "-v":
                        verbose = true;
                        break;
                    default:
                        System.err.println("Usage: java [-v] GoodLoader filename");
                        System.exit(1);
                }
                fileName = args[1];
                break;
            default:
                System.err.println("Usage: java [-v] GoodLoader filename");
                System.exit(1);
        }

        if (propertyurl == null) {
            System.err.println("No configuration file (loa" +
                    "der.cnf) found");
            System.exit(1);
        }
        Properties defprop = new Properties();
        defprop.put("host", "localhost");
        defprop.put("user", "leolu");
        defprop.put("password", "");
        defprop.put("database", "project");
        Properties prop = new Properties(defprop);


        try (BufferedReader infile = new BufferedReader(new FileReader(fileName))) {
            long start, end;
            String line;
//            infile.readLine();
            String[] data;
            int count = 0;

            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"), prop.getProperty("user"), prop.getProperty("password"));

            HashMap<String,Integer>sc_key = new HashMap<>();
            HashMap<String,Integer>client_key=new HashMap<>();
            HashMap<String,Integer>contract_key = new HashMap<>();
            HashMap<String,Integer>model_key = new HashMap<>();
            HashMap<Integer,Integer>salesman_key = new HashMap<>();
            int sc_id=1,client_id=1,contract_id=1,model_id=1,salesman_id=1;


            infile.readLine();
            while ((line = infile.readLine()) != null) {
                data = line.split(",");
                //0:contract number,1:client enterprise,2:supply center,
                // 3:country,4:city,5:industry,6:product code,7:product name,
                // 8:product model,9:unit price,10:quantity,11:contract date,
                // 12:estimated delivery date,13:lodgement date,14:director,
                // 15:salesman,16:salesman number,17:gender,18:age,19:mobile phone

                if (!sc_key.containsKey(data[2])){
                    sc_key.put(data[2],sc_id);
                    loadsupplycenter(sc_id++,data[2],data[14]);
                }
                if (!client_key.containsKey(data[1])){
                    client_key.put(data[1],client_id);
                    loadclient(client_id++,data[1],data[3],data[4],data[5],sc_key.get(data[2]));
                }
                if (!contract_key.containsKey(data[0])){
                    contract_key.put(data[0],contract_id);
                    loadcontract(contract_id++,data[0],data[11],client_key.get(data[1]));
                }
                if (!model_key.containsKey(data[8])){
                    model_key.put(data[8],model_id);
                    loadproduct_model(model_id++,data[8],data[7],data[6],Integer.parseInt(data[9]));
                }
                if (!salesman_key.containsKey(Integer.parseInt(data[16]))){
                    salesman_key.put(Integer.parseInt(data[16]),salesman_id++);
                    loadsalesman(Integer.parseInt(data[16]),data[15],data[17],Integer.parseInt(data[18]),data[19]);
                }
                loadorder(Integer.parseInt(data[10]),data[13],data[12],Integer.parseInt(data[16]),model_key.get(data[8]),contract_key.get(data[0]));

                count++;
                if (count % batch_size == 0) {
                    supplycenter.executeBatch();
                    client.executeBatch();
                    contract.executeBatch();
                    product_model.executeBatch();
                    salesman.executeBatch();
                    order.executeBatch();
                }
            }
            if (count % batch_size != 0) {
                product_model.executeBatch();
                salesman.executeBatch();
                supplycenter.executeBatch();
                client.executeBatch();
                contract.executeBatch();
                order.executeBatch();
            }
            con.commit();
            order.close();
            product_model.close();
            salesman.close();
            contract.close();
            client.close();
            supplycenter.close();
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
                order.close();
                product_model.close();
                salesman.close();
                contract.close();
                client.close();
                supplycenter.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                order.close();
                product_model.close();
                salesman.close();
                contract.close();
                client.close();
                supplycenter.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }
        closeDB();

    }
}
