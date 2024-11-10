package io.jcervelin;

public class MemoryLogger {

    public static void logMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();

        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory();

        System.out.printf("Used Memory: %d MB, Free Memory: %d MB, Total Memory: %d MB, Max Memory: %d MB%n",
                usedMemory / (1024 * 1024),
                freeMemory / (1024 * 1024),
                totalMemory / (1024 * 1024),
                maxMemory / (1024 * 1024));
    }
}
