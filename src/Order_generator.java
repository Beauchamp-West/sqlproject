import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.time.*;

public class Order_generator {
    static final File CWD = new File(System.getProperty("user.dir"));

    public static void main(String[] args) {
        Random random = new Random();
        generate_rand_orders(50000000,random);
    }

    static void generate_rand_orders(int num, Random random) {
        int quantity;
        String lodgement_date;
        String estimated_delivery_date;
        int salesman_number;
        int model_id;
        int contract_id;
        StringBuilder sb = new StringBuilder();

        File order = new File(CWD,"random_order.csv");

        for (int i = 0; i < num; i++) {
            StringJoiner sj = new StringJoiner(",");
            quantity = (random.nextInt(99)+1)*10;
            LocalDate l_date = nextDate(random,LocalDate.of(2000,1,1));
            LocalDate e_date = aroundDate(random,l_date);
            lodgement_date = dateToString(l_date);
            estimated_delivery_date = dateToString(e_date);
            salesman_number = random.nextInt(1110000)+11110000;
            model_id = 1+random.nextInt(1000);
            contract_id = 1+random.nextInt(3000);
            sj.add(String.valueOf(quantity)).add(lodgement_date).add(estimated_delivery_date)
                    .add(String.valueOf(salesman_number)).add(String.valueOf(model_id)).add(String.valueOf(contract_id));
            sb.append(sj.toString()).append("\n");

            if (i % 5000 == 0) {
                try {FileWriter writer = new FileWriter(order,true);
                    writer.write(sb.toString());
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                sb = new StringBuilder();
            }
        }

        try {FileWriter writer = new FileWriter(order,true);
            writer.write(sb.toString());
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static LocalDate nextDate(Random random, LocalDate minDate) {
        LocalDate maxDate = LocalDate.now();
        long minDay = minDate.toEpochDay();
        long maxDay = maxDate.toEpochDay();
        long randomDay = minDay + random.nextInt((int)(maxDay-minDay));
        return LocalDate.ofEpochDay(randomDay);
    }

    public static LocalDate aroundDate(Random random, LocalDate date) {
        LocalDate maxDate = LocalDate.now();
        long minDay = date.toEpochDay()-50;
        long maxDay = date.toEpochDay()+50;
        long randomDay = minDay + random.nextInt((int)(maxDay-minDay));
        return LocalDate.ofEpochDay(randomDay);
    }

    public static String dateToString(LocalDate date) {
        return String.format("%4d-%02d-%02d",date.getYear(),date.getMonthValue(),date.getDayOfMonth());
    }
}
