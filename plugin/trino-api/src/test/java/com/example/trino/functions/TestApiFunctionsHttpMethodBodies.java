package com.example.trino.functions;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestApiFunctionsHttpMethodBodies
{
    private HttpServer server;
    private String baseUrl;

    @BeforeEach
    public void setUp() throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/echo", this::echoHandler);
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/echo";
    }

    @AfterEach
    public void tearDown()
    {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testGetWithoutBodyStaysEmpty()
    {
        String response = invoke("GET", "", "/get-empty");
        assertEquals("method=GET;len=0;body=", response);
    }

    @Test
    public void testGetWithJsonBodyIsSent()
    {
        String response = invoke("GET", "{}", "/get-json");
        assertEquals("method=GET;len=2;body={}", response);
    }

    @Test
    public void testPatchWithBodyIsSent()
    {
        String response = invoke("PATCH", "{}", "/patch-json");
        assertEquals("method=PATCH;len=2;body={}", response);
    }

    @Test
    public void testDeleteWithBodyIsSent()
    {
        String response = invoke("DELETE", "{}", "/delete-json");
        assertEquals("method=DELETE;len=2;body={}", response);
    }

    @Test
    public void testPostWithEmptyBodyStillSendsZeroLengthBody()
    {
        String response = invoke("POST", "", "/post-empty");
        assertEquals("method=POST;len=0;body=", response);
    }

    private String invoke(String method, String body, String marker)
    {
        Slice result = ApiFunctions.callApi(
                Slices.utf8Slice(baseUrl + marker),
                Slices.utf8Slice(""),
                Slices.utf8Slice(""),
                Slices.utf8Slice("none"),
                Slices.utf8Slice(method),
                Slices.utf8Slice("{}"),
                Slices.utf8Slice(body));
        return result.toStringUtf8();
    }

    private void echoHandler(HttpExchange exchange) throws IOException
    {
        byte[] bytes = readFully(exchange.getRequestBody());
        String payload = "method=" + exchange.getRequestMethod() + ";len=" + bytes.length + ";body=" + new String(bytes, StandardCharsets.UTF_8);
        byte[] out = payload.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, out.length);
        exchange.getResponseBody().write(out);
        exchange.close();
    }

    private static byte[] readFully(InputStream inputStream) throws IOException
    {
        return inputStream.readAllBytes();
    }
}
