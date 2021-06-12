package air.art.concurrent.executor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DamoExecutor {

    public static class RunTask implements Runnable {
        @Override
        public void run() {
            int i = 1;
            while (true) {
                i=i*i;
                i++;
                System.out.println("I " + i + "@" + Thread.currentThread());
            }
        }
    }

    public static void main(String[] args) {

        RunTask task1 = new RunTask();

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                3,
                4,
                1,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(3)
        );

        threadPoolExecutor.execute(task1);
        threadPoolExecutor.execute(task1);
        threadPoolExecutor.execute(task1);
        threadPoolExecutor.execute(task1);
        threadPoolExecutor.execute(task1);
        threadPoolExecutor.execute(task1);
    }
}
