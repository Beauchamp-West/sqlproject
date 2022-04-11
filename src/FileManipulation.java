import java.io.*;
import java.util.*;


public class FileManipulation {

    static final String order_table = Utils.join("tables","order.csv");
    static final String insert_orders = Utils.join("tables","insert_data.csv"); // 20,000 orders.
    static final File CWD = new File(System.getProperty("user.dir"));
    static final String id_order = Utils.join("databases","id_order_map");
    static final File order_map = new File(CWD, id_order);
    static final String q_id = Utils.join("databases","quantity_id_map");
    static final File q_file = new File(CWD, q_id);
    static final String l_id = Utils.join("databases","l_date_id_map");
    static final File l_file = new File(CWD, l_id);
    static final String e_id = Utils.join("databases","e_date_id_map");
    static final File e_file = new File(CWD, e_id);
    static final String s_id = Utils.join("databases","salesman_id_map");
    static final File s_file = new File(CWD, s_id);
    static final String m_id = Utils.join("databases","model_id_map");
    static final File m_file = new File(CWD, m_id);
    static final String c_id = Utils.join("databases","c_id_map");
    static final File c_file = new File(CWD, c_id);

    private static int order_index;
    private static HashMap<Integer,Order> id_order_map;
    private static HashMap<Integer,HashSet<Integer>> q_id_map;
    private static HashMap<String,HashSet<Integer>> l_id_map;
    private static HashMap<String,HashSet<Integer>> e_id_map;
    private static HashMap<Integer,HashSet<Integer>> s_id_map;
    private static HashMap<Integer,HashSet<Integer>> m_id_map;
    private static HashMap<Integer,HashSet<Integer>> c_id_map;

    public static void init() {
        id_order_map = new HashMap<>();
        q_id_map = new HashMap<>();
        l_id_map = new HashMap<>();
        e_id_map = new HashMap<>();
        s_id_map = new HashMap<>();
        m_id_map = new HashMap<>();
        c_id_map = new HashMap<>();
        order_index = 0;
        id_order_map.put(-1,new Order(0));
        String line;
        String[] splitArray;
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(order_table))) {
            bufferedReader.readLine();
            while ((line = bufferedReader.readLine()) != null) {
                splitArray = line.split(",");
                id_order_map.put(++order_index, new Order(splitArray));
                update_map(splitArray);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void add_order(int id,int quantity,String l_date,String e_date,int s_number,int m_id, int c_id) {
        Order order = new Order(id,quantity, l_date, e_date, s_number, m_id, c_id);
        id_order_map.put(++order_index,order);

        String [] array = new String[7];
        array[0] = String.valueOf(id);
        array[1] = String.valueOf(quantity);
        array[2] = l_date;
        array[3] = e_date;
        array[4] = String.valueOf(s_number);
        array[5] = String.valueOf(m_id);
        array[6] = String.valueOf(c_id);
        update_map(array);
    }

    public static void add_order(String line) {
        String [] splitarray = line.split(",");
        id_order_map.put(++order_index,new Order(splitarray));
        update_map(splitarray);
    }

    public static void add_order_from_table() {
        String line;
        String[] splitArray;
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(insert_orders))) {
//            bufferedReader.readLine();
            while ((line = bufferedReader.readLine()) != null) {
                add_order(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
        -- DELETE --
     */

    public static void del_order_by_id(HashSet<Integer> ids) {
        for (int id : ids) {
            remove_id_from_maps(id);
            id_order_map.remove(id);

        }
    }

    /* Delete the orders with the given column name COL and VALUE. */
    public static void del_order_by_col(String col, String value) {
        HashSet<Integer> ids;
        ids = get_ids_by_col(col, value);
        del_order_by_id(ids);
    }

    /*
        -- SELECT --
     */

    /* Select the orders with the given column name COL and VALUE. */
    public static String select_order_by_id(HashSet<Integer> ids) {
        StringBuilder sb = new StringBuilder();
        sb.append(Order.get_col_names()).append("\n");
        String order = "";
        for (int id : ids) {
            order = id_order_map.get(id).toString();
            sb.append(order).append("\n");
        }
        return sb.toString();
    }

    /* Select the orders with the given column name COL and VALUE. */
    public static String select_order_by_col(String col, String value) {
        HashSet<Integer> ids = get_ids_by_col(col,value);
        return select_order_by_id(ids);
    }

    public static String select_cols_by_id(HashSet<Integer> ids, String... cols) {
        StringBuilder sb = new StringBuilder();
        StringJoiner col_names = new StringJoiner(",");
        for (String col : cols) {
            col_names.add(col);
        }
        sb.append(col_names).append("\n");

        for (int id : ids) {
            Order order = id_order_map.get(id);
            StringJoiner values = new StringJoiner(",");
            for (String col : cols) {
                values.add(order.get_col_value(col));
            }
            sb.append(values).append("\n");
        }

        return sb.toString();
    }

    /* Select some SELECTED_COLS of orders with the given COL and VALUE. */
    public static String select_cols_by_col(String col, String value, String... selected_cols) {
        HashSet<Integer> ids = get_ids_by_col(col,value);
        return select_cols_by_id(ids, selected_cols);
    }

    /*
        --UPDATE --
     */

    /* Update the order table with the given ID, UPDATED_COL and  UPDATED_VALUE. */
    public static void update_order_by_id(int id, String updated_col, String updated_value) {
        Order order = id_order_map.get(id);
        order.set_col_values(updated_col,updated_value);
        id_order_map.put(id,order);
        update_map_with_new_value(id,updated_col,updated_value);
    }

    /* Update the UPDATED_COL of orders which have VALUE in COL with
       given UPDATED_VALUE. */
    public static void update_order_by_col(String updated_col, String updated_val, String col, String value) {
        HashSet<Integer> ids = get_ids_by_col(col, value);
        for (int id : ids) {
            update_order_by_id(id,updated_col,updated_val);
        }
    }

    /*
        -- COUNT --
     */

    /* Return the total number of orders. */
    public static int get_orders_count() {
        return (id_order_map.size()-1);
    }

    /* Return the biggest index of orders. */
    public static int get_order_index() {
        return order_index;
    }

    /*
        -- DATA STORAGE --
     */

    /* Initialize an order table if it does not exist and load the table otherwise. */
    public static void open() {
        if (!order_map.exists()) {
            init();
        } else {
            load_order();
        }
    }

    /* Store ID_ORDER_MAP and ORDER_INDEX in a binary file. */
    public static void save_order() {
        id_order_map.put(-1,new Order(order_index));
        Utils.writeObject(order_map, id_order_map);
        Utils.writeObject(q_file,q_id_map);
        Utils.writeObject(l_file, l_id_map);
        Utils.writeObject(e_file,e_id_map);
        Utils.writeObject(s_file, s_id_map);
        Utils.writeObject(m_file,m_id_map);
        Utils.writeObject(c_file,c_id_map);

    }

    /* Load ID_ORDER_MAP and ORDER_INDEX from a binary file. */
    public static void load_order() {
        id_order_map = Utils.readObject(order_map,HashMap.class);
        order_index = id_order_map.get(-1).quantity;
        q_id_map = Utils.readObject(q_file,HashMap.class);
        l_id_map = Utils.readObject(l_file,HashMap.class);
        e_id_map = Utils.readObject(e_file,HashMap.class);
        s_id_map = Utils.readObject(s_file,HashMap.class);
        m_id_map = Utils.readObject(m_file,HashMap.class);
        c_id_map = Utils.readObject(c_file,HashMap.class);
    }

    /* Clear the order table. */
    public static void clear_order() {
        if (id_order_map != null) id_order_map.clear();
        if (q_id_map != null) q_id_map.clear();
        if (l_id_map != null) l_id_map.clear();
        if (e_id_map != null) e_id_map.clear();
        if (s_id_map != null) s_id_map.clear();
        if (m_id_map != null) m_id_map.clear();
        if (c_id_map != null) c_id_map.clear();
        order_index = 0;
        save_order();
    }

    /*
        --HELPER METHODS--
     */

    /* Update the maps from order columns to id by inputting members of an order COLS. */
    private static void update_map(String[] cols) {
        int quant = Integer.parseInt(cols[1]);
        String l_date = cols[2];
        String e_date = cols[3];
        int s_num = Integer.parseInt(cols[4]);
        int m_id = Integer.parseInt(cols[5]);
        int c_id = Integer.parseInt(cols[6]);
        HashSet<Integer> set;

        if (!q_id_map.containsKey(quant)) {
            q_id_map.put(quant,new HashSet<>());
        } else {
            set = q_id_map.get(quant);
            set.add(order_index);
            q_id_map.put(quant,set);
        }
        if (!l_id_map.containsKey(l_date)) {
            l_id_map.put(l_date,new HashSet<>());
        } else {
            set = l_id_map.get(l_date);
            set.add(order_index);
            l_id_map.put(l_date,set);
        }
        if (!e_id_map.containsKey(e_date)) {
            e_id_map.put(e_date,new HashSet<>());
        } else {
            set = e_id_map.get(e_date);
            set.add(order_index);
            e_id_map.put(e_date,set);
        }
        if (!s_id_map.containsKey(s_num)) {
            s_id_map.put(s_num,new HashSet<>());
        } else {
            set = s_id_map.get(s_num);
            set.add(order_index);
            s_id_map.put(s_num,set);
        }
        if (!m_id_map.containsKey(m_id)) {
            m_id_map.put(m_id,new HashSet<>());
        } else {
            set = m_id_map.get(m_id);
            set.add(order_index);
            m_id_map.put(m_id,set);
        }
        if (!c_id_map.containsKey(c_id)) {
            c_id_map.put(c_id,new HashSet<>());
        } else {
            set = c_id_map.get(c_id);
            set.add(order_index);
            c_id_map.put(c_id,set);
        }
    }

    /* Get the corresponding order ids with the given COL and VALUE. */
    private static HashSet<Integer> get_ids_by_col(String col, String value) {
        HashSet<Integer> ids;
        switch (col) {
            case "order_id":
                ids = new HashSet<>();
                ids.add(Integer.parseInt(value));
                break;
            case "quantity":
                ids = q_id_map.get(Integer.parseInt(value));
                break;
            case "lodgement_date":
                ids = l_id_map.get(value);
                break;
            case "estimated_delivery_date":
                ids = e_id_map.get(value);
                break;
            case "salesman_number":
                ids = s_id_map.get(Integer.parseInt(value));
                break;
            case "model_id":
                ids = m_id_map.get(Integer.parseInt(value));
                break;
            case "contract_id":
                ids = c_id_map.get(Integer.parseInt(value));
                break;
            default:
                ids = new HashSet<>();
                System.err.println("Illegal column name");
        }
        return ids;
    }

    /* Remove the ID in all the maps. */
    private static void remove_id_from_maps(int id) {
        Order order = id_order_map.get(id);
        int quantity = order.quantity;
        String l_date = order.lodgement_date;
        String e_date = order.estimated_delivery_date;
        int s_num = order.salesman_number;
        int m_id = order.model_id;
        int c_id = order.contract_id;

        HashSet<Integer> ids;
        ids = q_id_map.get(quantity);
        ids.remove(id);
        q_id_map.put(quantity,ids);

        ids = l_id_map.get(l_date);
        ids.remove(id);
        l_id_map.put(l_date,ids);

        ids = e_id_map.get(e_date);
        ids.remove(id);
        e_id_map.put(e_date,ids);

        ids = s_id_map.get(s_num);
        ids.remove(id);
        s_id_map.put(s_num,ids);

        ids = m_id_map.get(m_id);
        ids.remove(id);
        m_id_map.put(m_id,ids);

        ids = c_id_map.get(c_id);
        ids.remove(id);
        c_id_map.put(c_id,ids);
    }

    /* Update the COL_id_map by removing the ID in the set corresponding to the old value
    * and adding ID to the set corresponding to NEW_VALUE. */
    private static void update_map_with_new_value(int id, String col, String new_value) {
        Order order = id_order_map.get(id);
        HashSet<Integer> ids;
        HashSet<Integer> new_ids;
        switch (col) {
            case "quantity":
                int q = order.quantity;
                ids = q_id_map.get(q);
                ids.remove(id);
                q_id_map.put(q,ids);
                int new_q = Integer.parseInt(new_value);
                new_ids = q_id_map.get(new_q);
                new_ids.add(id);
                q_id_map.put(new_q,new_ids);
                break;
            case "lodgement_date":
                String l = order.lodgement_date;
                ids = l_id_map.get(l);
                ids.remove(id);
                l_id_map.put(l,ids);
                new_ids = l_id_map.get(new_value);
                new_ids.add(id);
                l_id_map.put(new_value,new_ids);
                break;
            case "estimated_delivery_date":
                String e = order.estimated_delivery_date;
                ids = e_id_map.get(e);
                ids.remove(id);
                e_id_map.put(e,ids);
                new_ids = e_id_map.get(new_value);
                new_ids.add(id);
                e_id_map.put(new_value,new_ids);
                break;
            case "salesman_number":
                int s = order.salesman_number;
                ids = s_id_map.get(s);
                ids.remove(id);
                s_id_map.put(s,ids);
                int new_s = Integer.parseInt(new_value);
                new_ids = s_id_map.get(new_s);
                new_ids.add(id);
                s_id_map.put(new_s,new_ids);
                break;
            case "model_id":
                int m = order.model_id;
                ids = m_id_map.get(m);
                ids.remove(id);
                m_id_map.put(m,ids);
                int new_m = Integer.parseInt(new_value);
                new_ids = m_id_map.get(new_m);
                new_ids.add(id);
                m_id_map.put(new_m,new_ids);
                break;
            case "contract_id":
                int c = order.contract_id;
                ids = c_id_map.get(c);
                ids.remove(id);
                c_id_map.put(c,ids);
                int new_c = Integer.parseInt(new_value);
                new_ids = c_id_map.get(new_c);
                new_ids.add(id);
                c_id_map.put(new_c,new_ids);
                break;
            default:
                System.err.println("Illegal column name");
        }
    }
}

