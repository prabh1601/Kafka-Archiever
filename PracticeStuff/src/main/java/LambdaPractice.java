public class LambdaPractice {
    @FunctionalInterface
    public interface greetings {
        public void greet();
    }

    public class Greet implements greetings {
        public void greet() {
            System.out.println("This is normal Implementation");
        }
    }

    @FunctionalInterface
    public interface calculator {
        public int add(int a, int b);
    }

    void run() {
        Greet g1 = new Greet();
        g1.greet();

        Greet g2 = new Greet() {
            public void greet() {
                System.out.println("This is using interface");
            }
        };

        g2.greet();

        greetings g3 = () -> System.out.println("This is lambda method");
        g3.greet();

        calculator c = (int a, int b) -> {
            return (a + 2 * b);
        };
    }

    public static void main(String[] args) {
        new LambdaPractice().run();
    }
}
