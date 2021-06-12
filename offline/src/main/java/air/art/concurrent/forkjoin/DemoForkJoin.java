package air.art.concurrent.forkjoin;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class DemoForkJoin {

    private static final Integer MAX =200;

    static class SumForkJoinTask extends RecursiveTask<Integer> {
        private Integer startValue;
        private Integer endValue;
        public  SumForkJoinTask(Integer startValue,Integer endValue) {
            this.startValue = startValue;
            this.endValue = endValue;
        }

        @Override
        protected Integer compute() {
            if(endValue - startValue < MAX) {
                System.out.println("开始计算的部分：startValue = " + startValue + ";endValue = " + endValue);
                Integer totalValue = 0;
                for(int index = this.startValue ; index <= this.endValue  ; index++) {
                    totalValue += index;
                }
                return totalValue;
            }
            else {
                SumForkJoinTask subTask1 = new SumForkJoinTask(startValue, (startValue + endValue) / 2);
                subTask1.fork();
                SumForkJoinTask subTask2 = new SumForkJoinTask((startValue + endValue) / 2 + 1 , endValue);
                subTask2.fork();
                return subTask1.join() + subTask2.join();
            }
        }
    }

    public static void main(String[] args) {

        ForkJoinPool forkJoinPool = new ForkJoinPool();
        ForkJoinTask<Integer> taskFuture = forkJoinPool.submit(new SumForkJoinTask(1,1001));

        try {
            Integer result = taskFuture.get();
            System.out.println("Result = " + result);
        } catch (InterruptedException | ExecutionException e) { e.printStackTrace(System.out); }
    }

}
