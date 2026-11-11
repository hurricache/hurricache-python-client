import com.hurricache.client.FastCacheAsyncSimpleClient;
import com.hurricache.client.FastCacheAsyncStandaloneClient;
import com.hurricache.client.KeyValueUtils;
import com.hurricache.client.intf.*;
import com.hurricache.grpc.*;
import com.hurricache.utils.CompressionUtils;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

/** Current Java API writers and pinned Java wire-converter readers against a live server. */
public class LiveInterop {
    static byte[] data(int size, boolean random) {
        byte[] result = new byte[size];
        int value = 1234567;
        for (int i = 0; i < size; i++) {
            value ^= value << 13;
            value ^= value >>> 17;
            value ^= value << 5;
            result[i] = (byte)(random ? value & 255 : 42);
        }
        return result;
    }
    static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    static Variant value(byte[] value) { return Variant.ofBytes(value); }
    static Variant read(Value raw) {
        return KeyValueUtils.toVariant(ValueResponse.newBuilder().setValueUnordered(raw).build());
    }
    static void check(boolean result, String message) {
        if (!result) throw new AssertionError(message);
    }
    static List<Variant> mixed() {
        return List.of(Variant.none(), value(new byte[0]), Variant.ofLong(0), Variant.ofDouble(1.25));
    }
    static List<Variant> storedMixed() {
        return List.of(value(new byte[0]), Variant.ofLong(0), Variant.ofDouble(0), Variant.ofDouble(1.25), value(data(4096,false)));
    }
    public static void main(String[] args) throws Exception {
        String action = args[0], target = args[1], prefix = args[2];
        Path hintsFile = Path.of(args[3]);
        int split = target.lastIndexOf(':');
        String host = target.substring(0, split).replace("[", "").replace("]", "");
        int port = Integer.parseInt(target.substring(split+1));
        ManagedChannel channel = NettyChannelBuilder.forAddress(host, port).usePlaintext()
            .maxInboundMessageSize(64*1024*1024).build();
        var wire = HurriCacheGrpcServiceGrpc.newBlockingStub(channel).withDeadlineAfter(60, java.util.concurrent.TimeUnit.SECONDS);
        var client = new FastCacheAsyncStandaloneClient(new FastCacheAsyncSimpleClient(host, port, 77, Duration.ofSeconds(30)));
        Map<String, KeyHintData> hints = new LinkedHashMap<>();
        if (!action.equals("write")) for (String line : Files.readAllLines(hintsFile)) {
            String[] row = line.split(" ");
            hints.put(row[0], KeyHintData.of((int)Long.parseLong(row[2]), (int)Long.parseLong(row[1])));
        }
        try {
            var allCases = List.of("empty", "small", "at", "compressed", "random", "list", "ordered", "map", "orderedmap", "large", "largeorderedmap", "none", "int", "double", "mixed", "storedmixed", "atomic", "longkey", "weightmap", "weightset");
            var cases = args.length > 4 ? List.of(args[4].split(",")) : allCases;
            check(allCases.containsAll(cases), "unknown diagnostic case");
            for (String name : cases) {
                String key = prefix + "/" + name + (name.equals("longkey") ? "/" + "x".repeat(2048) : "");
                int size = name.equals("empty") ? 0 : name.equals("small") ? 1023 : name.equals("at") ? 1024 : 4096;
                if (action.equals("cleanup")) { client.remove(key, hints.get(name)).get(); continue; }
                if (action.equals("write")) {
                    KeyHintData hint;
                    switch (name) {
                        case "list": hint = client.createList(key, List.of(value(data(20,false)), value(data(4096,false)))).get(); break;
                        case "mixed": hint = client.createList(key, mixed()).get(); break;
                        case "storedmixed": hint = client.createList(key, storedMixed()).get(); break;
                        case "ordered": hint = client.createOrderedSet(key, List.of(OrderedVariant.ofBytes(data(4096,false),1), OrderedVariant.ofBytes(data(4097,false),-1L))).get(); break;
                        case "large": hint = client.createList(key, Collections.nCopies(1024,value(data(4096,true)))).get(); break;
                        case "largeorderedmap":
                            Map<OrderedPayload,Variant> entries = new LinkedHashMap<>();
                            for(int i=0;i<1024;i++) entries.put(OrderedPayload.of(new byte[]{0,(byte)(i>>>8),(byte)i,(byte)255},(long)i+1),value(data(4096,true)));
                            hint=client.createOrderedMap(key,entries).get();break;
                        case "map": hint = client.createMap(key, Map.of(Payload.of(bytes("entry")), value(data(4096,false)))).get(); break;
                        case "orderedmap": hint = client.createOrderedMap(key, Map.of(OrderedPayload.of(data(4096,false),1L), value(data(4096,true)), OrderedPayload.of(data(4097,false),2L), value(data(4097,true)))).get(); break;
                        case "weightmap":
                            hint = client.createOrderedMap(key, Map.of(OrderedPayload.of(bytes("dup"),1L), Variant.ofDouble(1.25), OrderedPayload.of(bytes("dup"),2L), Variant.ofLong(2))).get();
                            check(client.incrementOrder(key,hint,OrderedPayload.of(bytes("dup"),1L),4L,77,Duration.ofSeconds(30)).get(),"increment map order");
                            check(client.update(key,hint,OrderedPayload.of(bytes("dup"),2L),7L,77,Duration.ofSeconds(30)).get(),"absolute map order");
                            break;
                        case "weightset":
                            hint = client.createOrderedSet(key, List.of(OrderedVariant.ofBytes(bytes("dup"),1), OrderedVariant.ofBytes(bytes("dup"),2))).get();
                            check(client.incrementOrder(key,hint,OrderedPayload.of(bytes("dup"),1L),4L,77,Duration.ofSeconds(30)).get(),"increment set order");
                            check(client.update(key,hint,OrderedPayload.of(bytes("dup"),2L),7L,77,Duration.ofSeconds(30)).get(),"absolute set order");
                            break;
                        default:
                            Variant scalar = switch(name) {
                                case "none" -> Variant.none(); case "int" -> Variant.ofLong(Long.MIN_VALUE);
                                case "double" -> Variant.ofDouble(-0.0); case "atomic" -> Variant.ofLong(10);
                                default -> value(data(size,name.equals("random")));
                            };
                            hint = client.create(key, scalar).get();
                            if (name.equals("atomic")) client.add(key,hint,5L).get();
                    }
                    hints.put(name,hint);
                } else if (List.of("list","mixed","storedmixed","ordered","large","largeorderedmap","map","orderedmap","weightmap","weightset").contains(name)) {
                    GetRequest request = KeyValueUtils.buildGetReq(bytes(key), hints.get(name), 77, 1024);
                    var stream = wire.getContainer(request);
                    List<Variant> actual = new ArrayList<>();
                    List<Long> orders = new ArrayList<>();
                    List<byte[]> keys = new ArrayList<>();
                    while (stream.hasNext()) {
                        BatchValueResponse batch = stream.next();
                        for (Value v : batch.getValueUnorderedList()) actual.add(read(v));
                        for (OrderedValue v : batch.getValueOrderedList()) {
                            actual.add(CompressionUtils.decompressIfNeededToVariant(v)); orders.add(v.getOrder());
                        }
                        for (Key k : batch.getKeyUnorderedList()) keys.add(CompressionUtils.decompressIfNeeded(Value.newBuilder()
                            .setValue(BinaryPayload.newBuilder().setPayload(k.getPayload().getPayload()).setSize(k.getPayload().getSize()))
                            .setCompressionInfo(k.getCompressionInfo()).build()));
                        for (OrderedKey k : batch.getKeyOrderedList()) { keys.add(CompressionUtils.decompressIfNeeded(k)); orders.add(k.getOrder()); }
                    }
                    if (name.equals("mixed")) check(actual.equals(mixed()),"mixed list");
                    if (name.equals("storedmixed")) check(actual.equals(storedMixed()),"stored mixed list");
                    if (name.equals("list")) check(actual.equals(List.of(value(data(20,false)),value(data(4096,false)))),"list");
                    if (name.equals("large")) check(actual.equals(Collections.nCopies(1024,value(data(4096,true)))),"continuation list");
                    if (name.equals("largeorderedmap")) {
                        check(actual.size()==1024 && orders.size()==1024 && keys.size()==1024,"continuation map count");
                        for(int i=0;i<1024;i++)check(orders.get(i)==i+1L && actual.get(i).equals(value(data(4096,true)))
                            && Arrays.equals(keys.get(i),new byte[]{0,(byte)(i>>>8),(byte)i,(byte)255}),"continuation map pair/order "+i);
                    }
                    if (name.equals("map")) check(actual.equals(List.of(value(data(4096,false)))) && Arrays.equals(keys.get(0),bytes("entry")),"map pairing");
                    if (name.equals("ordered")) check(actual.equals(List.of(value(data(4096,false)),value(data(4097,false)))) && orders.equals(List.of(1L,-1L)),"ordered set");
                    if (name.equals("orderedmap")) {
                        check(actual.size()==2,"ordered map count");
                        for(int i=0;i<2;i++) {
                            int length=4095+orders.get(i).intValue();
                            check(Arrays.equals(keys.get(i),data(length,false)) && actual.get(i).equals(value(data(length,true))),"ordered map pairing");
                        }
                    }
                    if (name.equals("weightmap") || name.equals("weightset")) {
                        check(orders.equals(List.of(5L,7L)),"updated order traversal");
                        if (name.equals("weightmap")) {
                            check(actual.equals(List.of(Variant.ofDouble(1.25),Variant.ofLong(2))),"order update preserves map payload");
                            check(keys.size()==2 && Arrays.equals(keys.get(0),bytes("dup")) && Arrays.equals(keys.get(1),bytes("dup")),"duplicate map keys");
                        } else check(actual.equals(List.of(value(bytes("dup")),value(bytes("dup")))),"order update preserves set members");
                        check(client.incrementOrder(key,hints.get(name),OrderedPayload.of(bytes("dup"),5L),1L,77,Duration.ofSeconds(30)).get(),"reader increments order");
                        check(client.update(key,hints.get(name),OrderedPayload.of(bytes("dup"),6L),5L,77,Duration.ofSeconds(30)).get(),"reader restores order");
                    }
                } else {
                    Variant expected = switch(name) {
                        case "none" -> Variant.none(); case "int" -> Variant.ofLong(Long.MIN_VALUE);
                        case "double" -> Variant.ofDouble(-0.0); case "atomic" -> Variant.ofLong(15);
                        default -> value(data(size,name.equals("random")));
                    };
                    check(client.get(key,hints.get(name)).get().equals(expected), name);
                    if (name.equals("atomic")) check(client.exchange(key,hints.get(name),15L).get().longValue()==15,"atomic exchange");
                }
            }
            if (action.equals("write")) {
                StringBuilder text = new StringBuilder();
                hints.forEach((name,hint) -> text.append(name).append(' ').append(Integer.toUnsignedString(hint.getWeek_hash()))
                    .append(' ').append(Integer.toUnsignedString(hint.getStrong_hash())).append('\n'));
                Files.writeString(hintsFile,text);
            }
        } finally { client.shutdown(); channel.shutdownNow(); }
        System.out.println("Java live " + action + " passed (" + System.getProperty("hurricache.compression", "gzip") + "; " + (args.length > 4 ? args[4] : "full") + ")");
    }
}
