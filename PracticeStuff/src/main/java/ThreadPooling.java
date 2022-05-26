import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPooling extends Thread {
    public void run() {
        System.out.println(Thread.currentThread().getName() + " chopped a vegetable");
    }
}

class demo {
    public static void main(String[] args) {
        ExecutorService pool = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 5; i++) {
            pool.submit(new ThreadPooling());
        }

        pool.shutdown();
    }
}