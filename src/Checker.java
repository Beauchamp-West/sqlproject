import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Checker {
    public static void main(String[] args) {
        String fileName = "file_name.csv";//导出的数据表
        String originalFile = "contract_info.csv";//原始表格
        int sum=0;
        try (BufferedReader infile = new BufferedReader(new FileReader(fileName));
        BufferedReader origin = new BufferedReader(new FileReader(originalFile))){
            infile.readLine();
            origin.readLine();
            String line1;
            String line2;

            while ((line1 = infile.readLine())!=null && (line2 = origin.readLine())!=null){
                String[] arr = line1.split(","),brr = line2.split(",");
                //contract number col 13, model name col 8 in file
                //contract number col 0, model name col 7 in origin file
                sum++;
                if (!arr[13].equals(brr[0])&&arr[8].equals(brr[7])){
                    System.err.println("Miss match in row "+sum);
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(sum+" rows matched");
    }
}
