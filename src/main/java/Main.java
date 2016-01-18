import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by yachironi on 15/12/15.
 */
public class Main {
    public static void main(String[] args) {
        Object[] tab = {3.2D, 22.01D, new Date()};
        List<Date> dates = new ArrayList<>();
        List<Double> doubles = new ArrayList<>();

        for (int i = 0; i < tab.length; i++) {
            try {
                double doubleVar = (Double) tab[i];
                doubles.add(doubleVar);
            } catch (ClassCastException e) {
                try {
                    Date dateVar = (Date) tab[i];
                    dates.add(dateVar);
                } catch (Exception e2) {
                    System.out.printf("No cast possible");
                }
            }
        }
    }


}
