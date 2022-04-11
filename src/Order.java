import java.io.Serializable;
import java.util.StringJoiner;

public class Order implements Serializable {
    int id;
    int quantity;
    String lodgement_date;
    String estimated_delivery_date;
    int salesman_number;
    int model_id;
    int contract_id;
    static String [] col_names = {"order_id","quantity","lodgement_date","estimated_delivery_date"
            ,"salesman_number","model_id","contract_id"};

    public Order(int id,int quantity,String l_date,String e_date,int s_number,int m_id, int c_id) {
        this.quantity = quantity;
        lodgement_date = l_date;
        estimated_delivery_date = e_date;
        salesman_number = s_number;
        model_id = m_id;
        contract_id = c_id;
    }

    /* Initialize an Order by a String array {id,quantity,l_date,e_date,s_number,m_id,c_id}. */
    public Order(String [] parameters) {
        id = Integer.parseInt(parameters[0]);
        quantity = Integer.parseInt(parameters[1]);
        lodgement_date = parameters[2];
        estimated_delivery_date = parameters[3];
        salesman_number = Integer.parseInt(parameters[4]);
        model_id = Integer.parseInt(parameters[5]);
        contract_id = Integer.parseInt(parameters[6]);
    }

    /* Store an integer in the Order class,
       which is only used for storing the biggest index of orders in the map.
     */
    public Order(int index) {
        quantity = index;
    }

    public static String get_col_names() {
        StringJoiner sj = new StringJoiner(",");
        for (String col : col_names) {
            sj.add(col);
        }
        return sj.toString();
    }

    public String get_col_value(String col) {
        switch (col) {
            case "order_id":
                return String.valueOf(id);
            case "quantity":
                return String.valueOf(quantity);
            case "lodgement_date":
                return lodgement_date;
            case "estimated_delivery_date":
                return estimated_delivery_date;
            case "salesman_number":
                return String.valueOf(salesman_number);
            case "model_id":
                return String.valueOf(model_id);
            case "contract_id":
                return String.valueOf(contract_id);
            default:
                return "";
        }
    }

    public void set_col_values(String col, String value) {
        switch (col) {
            case "quantity":
                quantity = Integer.parseInt(value);
                break;
            case "lodgement_date":
                lodgement_date = value;
                break;
            case "estimated_delivery_date":
                estimated_delivery_date = value;
                break;
            case "salesman_number":
                salesman_number = Integer.parseInt(value);
                break;
            case "model_id":
                model_id = Integer.parseInt(value);
                break;
            case "contract_id":
                contract_id = Integer.parseInt(value);
                break;
            default:
        }
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner(",");
        sj.add(String.valueOf(id));
        sj.add(String.valueOf(quantity));
        sj.add(lodgement_date);
        sj.add(estimated_delivery_date);
        sj.add(String.valueOf(salesman_number));
        sj.add(String.valueOf(model_id));
        sj.add(String.valueOf(contract_id));
        return sj.toString();
    }
}
