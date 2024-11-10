package io.jcervelin;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.jcervelin.MemoryLogger.logMemoryUsage;
import static java.lang.System.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.groupingByConcurrent;

public class CalculateAverage {

    private static final long SIZE = 10_000_000;
    private static final char LINE_BREAK = '\n';


    public static void main(String[] args) throws IOException {
        String path = args[0];
        boolean isVirtual = true;
        try {
            isVirtual = Boolean.parseBoolean(args[1]);
            if (isVirtual) {
                out.println("Running on virtual threads");
            } else {
                out.println("Running on regular threads");
            }
        } catch (Exception e) {
            out.println("Running on virtual threads");
        }

        long start = currentTimeMillis();
        try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
            List<Split> splits = splitFile(file);

            Map<Place, TotalTemp> placeTotalTempMap = calculateTemperature(splits, file, isVirtual);

            Map<String, FormattedPlace> results = buildFormattedMap(placeTotalTempMap);
            out.println(results);
        }
        long end = currentTimeMillis();
        long total = end - start;
        out.println("Total time in ms: " + total);
        out.println("Total time in seconds: " + total / 1000);
    }

    private static record FormattedPlace(double min, double mean, double max) {

        public FormattedPlace(TotalTemp ma) {
            this(ma.min / 10.0, ((Math.round(ma.sum * 100.0) / 100.0) / (double) ma.count) / 10.0, ma.max / 10.0);
        }

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static Map<String, FormattedPlace> buildFormattedMap(Map<Place, TotalTemp> totalTempMap) {
        return totalTempMap.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> new String(entry.getKey().bytes, UTF_8),
                        entry -> new FormattedPlace(entry.getValue()),
                        (existing, replacement) -> existing,
                        TreeMap::new
                ));
    }

    private static Map<Place, TotalTemp> calculateTemperature(List<Split> splits, RandomAccessFile file, boolean isVirtual) {

        try (ExecutorService executorService = isVirtual ? Executors.newVirtualThreadPerTaskExecutor() :
                Executors.newCachedThreadPool()
        ) {
            out.println("Number of processors: " + Runtime.getRuntime().availableProcessors());
            out.println("Number of chunks: " + splits.size());
            List<Future<Map<Place, TotalTemp>>> list = splits.parallelStream()
                    .map(filePart -> executorService.submit(() -> parse(filePart, file))).toList();
            return list.stream()
                    .map(mapFuture -> {
                        try {
                            return mapFuture.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .flatMap(m -> m.entrySet().stream())
                    .collect(groupingByConcurrent(
                            Map.Entry::getKey,
                            Collectors.reducing(
                                    new TotalTemp(),
                                    Map.Entry::getValue,
                                    TotalTemp::merge)));
        }

    }
    static AtomicInteger counter = new AtomicInteger();

    private static Map<Place, TotalTemp> parse(Split split, RandomAccessFile file) {
        try {
            byte[] bytes = fromSplitToBytes(split, file);
            if (counter.incrementAndGet() % 100 == 0) {
                logMemoryUsage();
            }
            return bytesToPlaceMap(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<Place, TotalTemp> bytesToPlaceMap(byte[] bytes) {
        Map<Place, TotalTemp> totalTemp = new HashMap<>(500);
        int semicolonIndex = 0;
        int newLineIndex = -1;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == ';') {
                semicolonIndex = i;
            } else if (bytes[i] == '\n') {
                byte[] place = Arrays.copyOfRange(bytes, newLineIndex + 1, semicolonIndex);
                long temp = parseDouble(bytes, semicolonIndex + 1, i);
                TotalTemp measurement = new TotalTemp(temp);
                totalTemp.compute(new Place(place), (k, prevV) -> prevV == null ? measurement : prevV.merge(measurement));
                newLineIndex = i;
            }
        }
        return totalTemp;
    }

    private static long parseDouble(byte[] text, int start, int end) {
        return (long) (Double.parseDouble(new String(text, start, end - start, UTF_8)) * 10);
    }

    static List<Split> splitFile(RandomAccessFile file) throws IOException {
        long length = file.length();

        List<Split> splits = new ArrayList<>((int) (length / SIZE));
        long current = 0;
        long next;

        while (current < length) {
            if (current + SIZE > length) {
                next = length;
            } else {
                next = nextLinePosition(file, current + SIZE);
            }
            splits.add(new Split(current, next - current));
            current = next;
        }

        return splits;
    }

    private static long nextLinePosition(RandomAccessFile file, long currentPosition) throws IOException {
        file.seek(currentPosition);

        while (file.readByte() != LINE_BREAK) {
        }

        return file.getFilePointer();
    }

    record Split(long offsetStart, long length) {
    }

    private static class Place {
        byte[] bytes;
        int hash;

        public Place(byte[] place) {
            this.bytes = place;
            this.hash = Arrays.hashCode(bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            return Arrays.equals(bytes, ((Place) o).bytes);
        }
    }

    static class TotalTemp {
        private long count;
        private double min;
        private double max;
        private double sum;

        public TotalTemp() {
            count = 0;
            min = Double.MAX_VALUE;
            max = Double.MIN_VALUE;
            sum = 0;
        }

        public TotalTemp(long temp) {
            min = temp;
            max = temp;
            sum = temp;
            count = 1;
        }

        public TotalTemp merge(TotalTemp tt) {
            TotalTemp totalTemp = new TotalTemp();
            totalTemp.min = Math.min(min, tt.min);
            totalTemp.max = Math.max(max, tt.max);
            totalTemp.count = count + tt.count;
            totalTemp.sum = sum + tt.sum;
            return totalTemp;
        }
    }

    private static byte[] fromSplitToBytes(Split split, RandomAccessFile file) throws IOException {
        var bb = file.getChannel().map(FileChannel.MapMode.READ_ONLY, split.offsetStart(), split.length());
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        return bytes;
    }
}
