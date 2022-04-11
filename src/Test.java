import java.util.HashSet;

public class Test {
    public static void main(String[] args) {
        FileManipulation.open();

        int init_cnt = FileManipulation.get_orders_count();
        System.out.printf("The 'order' table has %d orders initially.\n",init_cnt);

        long start,end;
        start = System.currentTimeMillis();
        FileManipulation.add_order_from_table();
        end = System.currentTimeMillis();
        System.out.printf("The 'order' table has %d orders after inserting 20,000 " +
                "orders. ",FileManipulation.get_orders_count());
        System.out.printf("It costs %d ms.\n", end - start);

        HashSet<Integer> ids = new HashSet<>();
        for (int i=0; i<20000; i++) {
            ids.add(i + 29867);
        }
        start = System.currentTimeMillis();
        FileManipulation.del_order_by_id(ids);
        end = System.currentTimeMillis();
        System.out.printf("The 'order' table has %d orders after deleting the 20,000 " +
                "orders added before. ",FileManipulation.get_orders_count());
        System.out.printf("It costs %d ms.\n", end - start);
        System.out.printf("The biggest order index is %d now.\n",FileManipulation.get_order_index());
        System.out.println();

        System.out.println("select quantity,model_id,lodgement_date where order_id = 29866:");
        String order_item = FileManipulation.select_cols_by_col("order_id","29866",
                "quantity","model_id","lodgement_date");
        System.out.println(order_item);

        System.out.println("select * where model_id = 1:");
        String order = FileManipulation.select_order_by_col("model_id","1");
        System.out.println(order);

        start = System.currentTimeMillis();
        FileManipulation.update_order_by_col("quantity","20","model_id","1");
        end = System.currentTimeMillis();
        System.out.println("After updating the order with model_id = 1 by setting its quantity = 20:");
        order = FileManipulation.select_order_by_col("model_id","1");
        System.out.println(order);
        System.out.printf("It costs %d ms.\n",end-start);
//        FileManipulation.save_order();
    }
}
