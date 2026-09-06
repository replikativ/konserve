import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/** Fails nonzero for ANY failed binding assertion. Unlike the API survey. */
public final class DirectorySyncBindingProbe {
    static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    static void selfTest() throws Exception {
        for (String fault : new String[] {"none", "open", "flush", "close", "both", "invalid"}) {
            List<String> events = new ArrayList<>();
            IOException flushError = new IOException("flush");
            IOException closeError = new IOException("close");
            WindowsDirectorySync.Calls calls = new WindowsDirectorySync.Calls() {
                public long open(String path) throws IOException {
                    events.add("open");
                    if (fault.equals("open")) throw new IOException("open");
                    return fault.equals("invalid") ? -1L : 123L;
                }
                public void flush(long handle) throws IOException {
                    require(handle == 123L, "wrong flush handle"); events.add("flush");
                    if (fault.equals("flush") || fault.equals("both")) throw flushError;
                }
                public void close(long handle) throws IOException {
                    require(handle == 123L, "wrong close handle"); events.add("close");
                    if (fault.equals("close") || fault.equals("both")) throw closeError;
                }
            };
            IOException failure = null;
            try { WindowsDirectorySync.flushDirectory("unused", calls); }
            catch (IOException e) { failure = e; }
            require((failure == null) == fault.equals("none"), "failure lost: " + fault);
            require(events.equals(fault.equals("open") || fault.equals("invalid")
                ? List.of("open") : List.of("open", "flush", "close")), "bad lifecycle: " + events);
            if (fault.equals("both")) {
                require(failure == flushError, "primary flush failure lost");
                require(failure.getSuppressed().length == 1 && failure.getSuppressed()[0] == closeError,
                        "close failure not suppressed");
            }
            System.out.println("PASS lifecycle " + fault);
        }
    }

    public static void main(String[] args) throws Exception {
        selfTest();
        if (args.length == 1 && args[0].equals("--self-test")) return;
        if (args.length != 1) throw new IllegalArgumentException("scratch-parent or --self-test required");
        Path base = Files.createTempDirectory(Path.of(args[0]), "ffm-directory-");
        System.out.println("ENV os=" + System.getProperty("os.name") + " java="
            + System.getProperty("java.version") + " fs=" + Files.getFileStore(base).type());
        // Include Unicode and a >260-character absolute path.
        for (String scenario : new String[] {"ordinary", "unicode-\u03bb-\u6c34", "long"}) {
            Path dir = base.resolve(scenario);
            if (scenario.equals("long")) for (int i = 0; i < 6; i++) dir = dir.resolve("segment-" + "x".repeat(45));
            Files.createDirectories(dir);
            for (int revision = 0; revision < 8; revision++) {
                Path stage = dir.resolve("staged");
                try (FileChannel c = FileChannel.open(stage, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                    ByteBuffer value = ByteBuffer.allocate(8).putLong(revision); value.flip();
                    while (value.hasRemaining()) c.write(value);
                    c.force(true);
                }
                Path target = dir.resolve("value");
                Files.move(stage, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                WindowsDirectorySync.flush(dir);
                require(ByteBuffer.wrap(Files.readAllBytes(target)).getLong() == revision, "readback mismatch");
            }
            System.out.println("PASS directory-flush create/replace " + scenario);
        }
        try {
            WindowsDirectorySync.flush(base.resolve("missing"));
            throw new AssertionError("missing directory accepted");
        } catch (IOException expected) {
            require(expected.getMessage().contains("Win32="), "native error code missing");
            System.out.println("PASS missing-directory " + expected.getMessage());
        }
        System.out.println("PASS Windows binding; API/readback checks only, not crash durability");
    }
}
