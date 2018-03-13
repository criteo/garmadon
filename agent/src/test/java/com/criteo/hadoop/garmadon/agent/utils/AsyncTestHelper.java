package com.criteo.hadoop.garmadon.agent.utils;

public class AsyncTestHelper {

    /**
     * Helper to test async behavior. We expect that runnable throws exception
     * when assertions are false.
     * The method tests that eventually all assertions are true.
     * We avoid just checking once and exit if we have no exceptions. This is because after a while,
     * the async process might produce undesired behavior.
     * To avoid this we constantly try to run assertions and if after the time limit
     * there are no exceptions thrown, we assume that eventually the desired assertions were met
     */
    public static void tryDuring(int millis, Runnable c) {
        long deadline = System.currentTimeMillis() + millis;
        Throwable lastError = null;

        while(System.currentTimeMillis() < deadline){

            try {
                c.run();
                lastError = null;
            } catch (Throwable t){
                lastError = t;
            }

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if(lastError != null){
            throw new RuntimeException("could not verify assertions within "
                    + millis + " milliseconds. Last error was " + lastError.getMessage());
        }
    }

}
