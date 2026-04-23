package com.example.trino.functions;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestApiFunctionsTimeouts
{
    private HttpServer server;
    private String baseUrl;

    @BeforeEach
    public void setUp() throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/fast", this::fastHandler);
        server.createContext("/slow", this::slowHandler);
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    public void tearDown()
    {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testDefaultTimeoutConstants()
    {
        assertEquals(20, ApiFunctions.DEFAULT_CONNECT_TIMEOUT_SECONDS);
        assertEquals(300, ApiFunctions.DEFAULT_REQUEST_TIMEOUT_SECONDS);
    }

    @Test
    public void testOverrideRequestTimeoutTriggersError()
    {
        String result = callApi("/slow", 20, 1);
        assertTrue(result.startsWith("ERROR:"), result);
        assertTrue(result.toLowerCase().contains("timed out"), result);
    }

    @Test
    public void testOverrideTimeoutSucceedsWhenBudgetIsEnough()
    {
        String result = callApi("/slow", 20, 3);
        assertEquals("slow-ok", result);
    }

    @Test
    public void testInvalidTimeoutValuesAreRejected()
    {
        String result = callApi("/fast", 0, 1);
        assertEquals("ERROR: connect_timeout_seconds must be greater than 0", result);
    }

    private String callApi(String path, long connectTimeoutSeconds, long requestTimeoutSeconds)
    {
        Slice result = ApiFunctions.callApi(
                Slices.utf8Slice(baseUrl + path),
                Slices.utf8Slice(""),
                Slices.utf8Slice(""),
                Slices.utf8Slice("none"),
                Slices.utf8Slice("GET"),
                Slices.utf8Slice("{}"),
                Slices.utf8Slice(""),
                connectTimeoutSeconds,
                requestTimeoutSeconds);
        return result.toStringUtf8();
    }

    private void fastHandler(HttpExchange exchange) throws IOException
    {
        respond(exchange, "fast-ok");
    }

    private void slowHandler(HttpExchange exchange) throws IOException
    {
        try {
            Thread.sleep(1500);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        respond(exchange, "slow-ok");
    }

    private static void respond(HttpExchange exchange, String payload) throws IOException
    {
        byte[] bytes = payload.getBytes();
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
