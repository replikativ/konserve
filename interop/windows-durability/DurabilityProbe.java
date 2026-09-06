import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** API compatibility probe, NOT a power-loss or crash-durability test. */
public final class DurabilityProbe {
    private static final byte[] OLD = "old-complete-value".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NEW = "new-complete-value".getBytes(StandardCharsets.UTF_8);
    private static String runtime;

    private static String json(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"")
            .replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t") + "\"";
    }

    private static void row(String recipe, String scenario, String outcome, String detail) {
        System.out.println("{\"runtime\":" + json(runtime) + ",\"recipe\":" + json(recipe)
            + ",\"scenario\":" + json(scenario) + ",\"outcome\":" + json(outcome)
            + ",\"detail\":" + json(detail) + "}");
    }

    private static void write(Path path, byte[] bytes, boolean sync) throws Exception {
        try (FileChannel c = sync
                ? FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE,
                                   StandardOpenOption.SYNC)
                : FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer b = ByteBuffer.wrap(bytes);
            while (b.hasRemaining()) c.write(b);
            c.force(true);
        }
    }

    private static void probe(Path base, String recipe, String scenario) throws Exception {
        Path dir = Files.createDirectory(base.resolve(recipe + "-" + scenario));
        Path target = dir.resolve("value");
        Path stage = dir.resolve("staged");
        String phase = "seed";
        try {
            if (!scenario.equals("create")) write(target, OLD, false);
            try (FileChannel reader = scenario.equals("held-reader")
                    ? FileChannel.open(target, StandardOpenOption.READ) : null) {
                phase = "write-and-force-stage";
                write(stage, NEW, recipe.equals("sync-move-force"));
                phase = "atomic-move";
                Files.move(stage, target, StandardCopyOption.ATOMIC_MOVE,
                           StandardCopyOption.REPLACE_EXISTING);
                if (recipe.equals("force-move-force") || recipe.equals("sync-move-force")) {
                    phase = "force-destination";
                    try (FileChannel c = FileChannel.open(target, StandardOpenOption.WRITE)) {
                        c.force(true);
                    }
                }
                if (recipe.equals("force-move-dirforce")) {
                    phase = "force-directory";
                    try (FileChannel c = FileChannel.open(dir, StandardOpenOption.READ)) {
                        c.force(true);
                    }
                }
                phase = "verify";
                if (!Arrays.equals(NEW, Files.readAllBytes(target)))
                    throw new IllegalStateException("destination bytes differ");
                row(recipe, scenario, "ok", "API completion and readback only");
            }
        } catch (Exception e) {
            String state = Files.exists(target)
                ? (Arrays.equals(NEW, Files.readAllBytes(target)) ? "new" : "other") : "missing";
            row(recipe, scenario, "error", phase + ": " + e + "; target=" + state);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("usage: DurabilityProbe scratch-parent runtime-label");
        runtime = args[1];
        Path base = Files.createTempDirectory(Path.of(args[0]), "durability-probe-");
        row("environment", "metadata", "info", "os=" + System.getProperty("os.name")
            + "; os-version=" + System.getProperty("os.version")
            + "; java=" + System.getProperty("java.version")
            + "; filesystem=" + Files.getFileStore(base).type() + "; scratch=" + base);
        for (String recipe : new String[] {"force-move", "force-move-force",
                "force-move-dirforce", "sync-move-force"})
            for (String scenario : new String[] {"create", "replace", "held-reader"})
                probe(base, recipe, scenario);
        // Scratch is left for the runner's teardown; never delete a caller path.
    }
}
