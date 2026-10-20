import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.BatchValueResponse;
import com.hurricache.utils.CompressionUtils;
import com.hurricache.utils.StreamBatchOrderedMapObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Offline wire fixtures compiled against the unchanged pinned Java sources. */
public class CodecInterop {
    static byte[] data(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) data[i] = (byte) (i % 251);
        return data;
    }

    public static void main(String[] args) throws Exception {
        Path fixture = Path.of(args[1]);
        if (args[0].equals("write")) {
            BatchValueResponse.Builder batch = BatchValueResponse.newBuilder();
            for (long order : new long[]{2, 1}) {
                byte[] raw = data(4096 + (int) order);
                batch.addKeyOrdered(CompressionUtils.compressKeyIfNeeded(raw, order, 0, 1024));
                batch.addValueUnordered(CompressionUtils.compressIfNeeded(raw, 1024));
            }
            Files.write(fixture, batch.build().toByteArray());
        } else {
            CompletableFuture<Map<OrderedPayload, Payload>> future = new CompletableFuture<>();
            StreamBatchOrderedMapObserver observer = new StreamBatchOrderedMapObserver(future);
            observer.onNext(BatchValueResponse.parseFrom(Files.readAllBytes(fixture)));
            observer.onCompleted();
            Map<OrderedPayload, Payload> decoded = future.get();
            long[] expectedOrder = {2, 1};
            int index = 0;
            for (var entry : decoded.entrySet()) {
                long order = expectedOrder[index++];
                byte[] raw = data(4096 + (int) order);
                if (entry.getKey().getOrder() != order || !Arrays.equals(entry.getKey().getValue(), raw)
                    || !Arrays.equals(entry.getValue().getValue(), raw)) throw new AssertionError("ordered map decoding");
            }
            if (index != 2) throw new AssertionError("missing map pairs");
        }
        System.out.println("Java " + args[0] + " " + System.getProperty("hurricache.compression") + " fixture passed");
    }
}
