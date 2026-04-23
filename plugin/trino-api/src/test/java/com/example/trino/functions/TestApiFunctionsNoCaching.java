package com.example.trino.functions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.ScalarFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestApiFunctionsNoCaching
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final AtomicInteger requestCounter = new AtomicInteger();

    private HttpServer server;
    private String baseUrl;

    @BeforeEach
    public void setUp() throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/counter", this::counterHandler);
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/counter";
    }

    @AfterEach
    public void tearDown()
    {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testIdenticalCallsAreNotCached()
    {
        String first = invoke(false);
        String second = invoke(false);

        assertEquals("count=1", first);
        assertEquals("count=2", second);
        assertEquals(2, requestCounter.get());
    }

    @Test
    public void testIdenticalCallsWithMetadataAreNotCached() throws Exception
    {
        Map<String, Object> first = invokeMeta();
        Map<String, Object> second = invokeMeta();

        assertEquals(200, first.get("status"));
        assertEquals("count=1", first.get("body"));
        assertEquals(200, second.get("status"));
        assertEquals("count=2", second.get("body"));
        assertEquals(2, requestCounter.get());
    }

    @Test
    public void testCallApiIsDeclaredNonDeterministic()
    {
        Method[] methods = Arrays.stream(ApiFunctions.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("callApi"))
                .toArray(Method[]::new);

        assertEquals(4, methods.length);
        for (Method method : methods) {
            ScalarFunction annotation = method.getAnnotation(ScalarFunction.class);
            assertFalse(annotation.deterministic(), method + " must be non-deterministic");
        }
    }

    private String invoke(boolean returnMeta)
    {
        Slice result = ApiFunctions.callApi(
                Slices.utf8Slice(baseUrl),
                Slices.utf8Slice(""),
                Slices.utf8Slice(""),
                Slices.utf8Slice("none"),
                Slices.utf8Slice("GET"),
                Slices.utf8Slice("{}"),
                Slices.utf8Slice(""),
                returnMeta);
        return result.toStringUtf8();
    }

    private Map<String, Object> invokeMeta() throws Exception
    {
        return OBJECT_MAPPER.readValue(invoke(true), new TypeReference<>() {});
    }

    private void counterHandler(HttpExchange exchange) throws IOException
    {
        int count = requestCounter.incrementAndGet();
        byte[] payload = ("count=" + count).getBytes();
        exchange.sendResponseHeaders(200, payload.length);
        exchange.getResponseBody().write(payload);
        exchange.close();
    }
}
