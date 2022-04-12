import java.util.HashSet;

public class Test {
    public static void main(String[] args) {
        FileManipulation.open();
        FileManipulation.save_order();

        int init_cnt = FileManipulation.get_orders_count();
        System.out.printf("The 'order' table has %d orders initially.\n",init_cnt);

        long start,end;
        start = System.currentTimeMillis();
        FileManipulation.load_order();
        FileManipulation.add_order_from_table();
        FileManipulation.save_order();
        end = System.currentTimeMillis();
        System.out.printf("The 'order' table has %d orders after inserting 20,000 " +
                "orders. ",FileManipulation.get_orders_count());
        System.out.printf("It costs %d ms.\n", end - start);

        HashSet<Integer> ids = new HashSet<>();
        for (int i=0; i<20000; i++) {
            ids.add(i + 29867);
        }
        start = System.currentTimeMillis();
        FileManipulation.load_order();
        FileManipulation.del_order_by_id(ids);
        FileManipulation.save_order();
        end = System.currentTimeMillis();
        System.out.printf("The 'order' table has %d orders after deleting the 20,000 " +
                "orders added before. ",FileManipulation.get_orders_count());
        System.out.printf("It costs %d ms.\n", end - start);
//        System.out.printf("The biggest order index is %d now.\n",FileManipulation.get_order_index());
//        System.out.println();
        FileManipulation.add_order_from_table();

        System.out.print("Select orders where contract_id varies from 1 to 2000. ");
        String [] indexes = new String[5000];
        for (int i = 0; i < 5000; i++) {
            indexes[i] = String.valueOf(i+1);
        }
        start = System.currentTimeMillis();
        FileManipulation.load_order();
        for (int i = 0; i < 2000;i++) {
            FileManipulation.select_order_by_col("contract_id", indexes[i]);
//            System.out.println(order);
        }
        FileManipulation.save_order();
        end = System.currentTimeMillis();
        System.out.printf("It costs %d ms.\n", end - start);

        System.out.print("Update orders where model_id varies from 1 to 500. ");
        start = System.currentTimeMillis();
        FileManipulation.load_order();
        for (int i = 0; i < 500; i++) {
            FileManipulation.update_order_by_col("quantity", indexes[i], "model_id", indexes[i]);
        }
        FileManipulation.save_order();
        end = System.currentTimeMillis();
//        System.out.println("After updating the order with model_id = 1 by setting its quantity = 20:");
//        order = FileManipulation.select_order_by_col("model_id","1");
//        System.out.println(order);
        System.out.printf("It costs %d ms.\n",end-start);
//        FileManipulation.save_order();
    }
}
