import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class StreamsPractice {
    public static void main(String[] args) {
        // Task : Check if a country's name starts with 'C'
        List<String> countries = List.of("Columbia", "Antarctica", "wtf");

        // without streams
        for (String country : countries) {
            String temp = country.toUpperCase();
            if (temp.startsWith("C")) {
                System.out.println(country);
            }
        }

        // with streams
        countries.stream()
                .map(String::toUpperCase)
                .filter(s -> s.startsWith("C"))
                .sorted()
                .forEach(System.out::println);

        // yet another method to do streams
        Stream<String> s = Stream.of("Columbia", "Prabh", "Carolina", "Wtf");
        s.sorted().forEach(country -> System.out.println(country));

}
