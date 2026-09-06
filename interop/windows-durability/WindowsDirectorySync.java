import java.io.IOException;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import static java.lang.foreign.ValueLayout.*;

/** Experimental JDK 22+ binding. Not yet connected to Konserve's runtime. */
public final class WindowsDirectorySync {
    interface Calls {
        long open(String path) throws IOException;
        void flush(long handle) throws IOException;
        void close(long handle) throws IOException;
    }

    // Separated from native linkage so resource/error semantics can be tested
    // on Linux as well. No error is converted to successful completion.
    static void flushDirectory(String path, Calls calls) throws IOException {
        long handle = calls.open(path);
        if (handle == -1L || handle == 0L) throw new IOException("Invalid directory handle");
        Throwable primary = null;
        try {
            calls.flush(handle);
        } catch (IOException | RuntimeException | Error e) {
            primary = e;
            throw e;
        } finally {
            try { calls.close(handle); }
            catch (IOException | RuntimeException | Error e) {
                if (primary != null) primary.addSuppressed(e);
                else throw e;
            }
        }
    }

    public static void flush(Path directory) throws IOException {
        if (!System.getProperty("os.name", "").startsWith("Windows"))
            throw new UnsupportedOperationException("Windows-only directory flush");
        String path = directory.toAbsolutePath().normalize().toString();
        // Extended paths avoid MAX_PATH truncation; UNC retains its UNC prefix.
        if (!path.startsWith("\\\\?\\"))
            path = path.startsWith("\\\\") ? "\\\\?\\UNC\\" + path.substring(2) : "\\\\?\\" + path;
        flushDirectory(path, Native.INSTANCE);
    }

    private static final class Native implements Calls {
        static final Native INSTANCE = new Native();
        private final MethodHandle open, flush, close;
        private final StructLayout state = Linker.Option.captureStateLayout();
        private final long errorOffset = state.byteOffset(MemoryLayout.PathElement.groupElement("GetLastError"));

        Native() {
            if (ADDRESS.byteSize() != 8) throw new UnsupportedOperationException("Prototype requires Windows x64");
            Linker linker = Linker.nativeLinker();
            SymbolLookup kernel = SymbolLookup.libraryLookup("kernel32", Arena.global());
            Linker.Option capture = Linker.Option.captureCallState("GetLastError");
            open = linker.downcallHandle(kernel.find("CreateFileW").orElseThrow(),
                FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS,
                                      JAVA_INT, JAVA_INT, ADDRESS), capture);
            flush = linker.downcallHandle(kernel.find("FlushFileBuffers").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS), capture);
            close = linker.downcallHandle(kernel.find("CloseHandle").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS), capture);
        }

        private IOException error(String operation, MemorySegment captured) {
            return new IOException(operation + " failed: Win32="
                + Integer.toUnsignedString(captured.get(JAVA_INT, errorOffset)));
        }

        public long open(String path) throws IOException {
            try (Arena arena = Arena.ofConfined()) {
                // Windows WCHAR is UTF-16; allocateFrom adds the terminator.
                MemorySegment name = arena.allocateFrom(path, java.nio.charset.StandardCharsets.UTF_16LE);
                MemorySegment captured = arena.allocate(state);
                MemorySegment handle = (MemorySegment) open.invokeExact(captured, name,
                    0x40000000, 7, MemorySegment.NULL, 3, 0x02000000, MemorySegment.NULL);
                if (handle.address() == -1L || handle.address() == 0L)
                    throw error("CreateFileW(" + path + ")", captured);
                return handle.address();
            } catch (IOException e) { throw e; }
            catch (Throwable e) { throw failure(e); }
        }

        private void call(MethodHandle function, String name, long handle) throws IOException {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment captured = arena.allocate(state);
                int result = (int) function.invokeExact(captured, MemorySegment.ofAddress(handle));
                if (result == 0) throw error(name, captured);
            } catch (IOException e) { throw e; }
            catch (Throwable e) { throw failure(e); }
        }

        private IOException failure(Throwable e) {
            if (e instanceof Error error) throw error;
            if (e instanceof RuntimeException runtime) throw runtime;
            return new IOException("Foreign call failed", e);
        }

        public void flush(long handle) throws IOException { call(flush, "FlushFileBuffers", handle); }
        public void close(long handle) throws IOException { call(close, "CloseHandle", handle); }
    }
}
