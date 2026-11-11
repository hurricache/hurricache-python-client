import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import com.hurricache.client.KeyValueUtils;
import com.hurricache.grpc.BatchValueResponse;
import com.hurricache.grpc.Value;
import com.hurricache.grpc.ValueResponse;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.KeyHint;
import com.hurricache.grpc.BatchCommand;
import com.hurricache.grpc.BatchMessage;
import com.hurricache.grpc.BatchSingleMessage;
import com.hurricache.grpc.ReplicationCommandType;
import com.hurricache.grpc.UpdateContainerRequest;
import com.hurricache.utils.CompressionUtils;
import com.hurricache.utils.StreamBatchOrderedMapObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Offline wire fixtures compiled against the unchanged pinned Java sources. */
public class CodecInterop {
    static void updates(String action, Path base) throws Exception {
        Path fixture = Path.of(base + ".updates");
        if (action.equals("write")) {
            var absolute = UpdateContainerRequest.newBuilder()
                .setKey(KeyValueUtils.buildKey("container".getBytes(), null, 17, 1024))
                .setOrderedElementKey(KeyValueUtils.createOrderedKey(data(4096), 10, 17, 1024))
                .setNewOrder(0).build();
            var delta = absolute.toBuilder().setDeltaOrder(-5).build();
            var plain = UpdateContainerRequest.newBuilder()
                .setKey(absolute.getKey())
                .setElementKey(KeyValueUtils.createUnorderedKey("entry".getBytes(), 17, null))
                .setValue(KeyValueUtils.buildValueFromVariantCompressed(Variant.ofLong(0), 1024)).build();
            BatchMessage.Builder batch = BatchMessage.newBuilder();
            for (var update : new UpdateContainerRequest[]{absolute, delta, plain}) {
                batch.addBatchSingleMessages(BatchSingleMessage.newBuilder()
                    .setReplicationCommandType(ReplicationCommandType.UPDATE_IN_CONTAINER_BY_KEY)
                    .setBatchCommand(BatchCommand.newBuilder().setUpdateInContainerByKey(update)));
            }
            Files.write(fixture, batch.build().toByteArray());
        } else {
            var batch = BatchMessage.parseFrom(Files.readAllBytes(fixture));
            if (batch.getBatchSingleMessagesCount() != 3) throw new AssertionError("update count");
            for (int index=0; index<3; index++) {
                var update = batch.getBatchSingleMessages(index).getBatchCommand().getUpdateInContainerByKey();
                if (index<2) {
                    if (!update.hasOrderedElementKey() || update.hasValue() || update.getOrderedElementKey().getOrder()!=10
                        || update.getOrderedElementKey().getClientId()!=17
                        || !Arrays.equals(CompressionUtils.decompressIfNeeded(update.getOrderedElementKey()),data(4096)))
                        throw new AssertionError("ordered update selector");
                    if (index==0 && (!update.hasNewOrder() || update.getNewOrder()!=0)) throw new AssertionError("absolute zero order");
                    if (index==1 && (!update.hasDeltaOrder() || update.getDeltaOrder()!=-5)) throw new AssertionError("signed order delta");
                } else if (!update.hasElementKey() || !update.hasValue()
                    || !KeyValueUtils.toVariant(ValueResponse.newBuilder().setValueUnordered(update.getValue()).build()).equals(Variant.ofLong(0)))
                    throw new AssertionError("plain value field migration");
            }
        }
    }
    static Variant[] mixed() {
        return new Variant[]{Variant.none(), Variant.ofBytes(new byte[0]), Variant.ofLong(0), Variant.ofDouble(0),
            Variant.ofLong(Long.MIN_VALUE), Variant.ofLong(Long.MAX_VALUE), Variant.ofDouble(-0.0),
            Variant.ofDouble(1.25), Variant.ofBytes(data(4096)), Variant.ofDouble(Double.NaN)};
    }

    static void typed(String action, Path base) throws Exception {
        Path fixture = Path.of(base + ".typed");
        if (action.equals("write")) {
            BatchValueResponse.Builder batch = BatchValueResponse.newBuilder();
            for (Variant variant : mixed()) batch.addValueUnordered(KeyValueUtils.buildValueFromVariantCompressed(variant, 1024));
            Files.write(fixture, batch.build().toByteArray());
            Files.write(Path.of(base + ".cas"), AtomicCasRes.newBuilder().setResult(false).setExpected(
                ValueResponse.newBuilder().setValueUnordered(KeyValueUtils.buildValueFromVariantCompressed(Variant.ofDouble(-0.0), 1024))
                .setKeyHint(KeyHint.newBuilder().setWeakHash(0))).build().toByteArray());
        } else {
            BatchValueResponse batch = BatchValueResponse.parseFrom(Files.readAllBytes(fixture));
            if (batch.getValueUnorderedCount() != mixed().length) throw new AssertionError("typed count");
            int index = 0;
            for (Value value : batch.getValueUnorderedList()) {
                Variant actual = KeyValueUtils.toVariant(ValueResponse.newBuilder().setValueUnordered(value).build());
                if (!actual.equals(mixed()[index++])) throw new AssertionError("typed value " + index + ": " + actual);
            }
            AtomicCasRes cas = AtomicCasRes.parseFrom(Files.readAllBytes(Path.of(base + ".cas")));
            if (!cas.hasExpected() || !KeyValueUtils.toVariant(cas.getExpected()).equals(Variant.ofDouble(-0.0))
                || !cas.getExpected().getKeyHint().hasWeakHash()) throw new AssertionError("nested CAS");
        }
    }
    static byte[] data(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) data[i] = (byte) (i % 251);
        return data;
    }

    public static void main(String[] args) throws Exception {
        Path fixture = Path.of(args[1]);
        typed(args[0], fixture);
        updates(args[0], fixture);
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
