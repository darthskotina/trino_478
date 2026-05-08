package com.example.trino.functions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestApiFunctionsRequestFeatures
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private HttpServer server;
    private String baseUrl;

    @BeforeEach
    public void setUp() throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/auth/basic", this::basicAuthHandler);
        server.createContext("/auth/bearer", this::bearerAuthHandler);
        server.createContext("/headers", this::headersHandler);
        server.createContext("/meta", this::metaHandler);
        server.createContext("/redirect", this::redirectHandler);
        server.createContext("/redirect-target", this::redirectTargetHandler);
        server.createContext("/method", this::methodHandler);
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
    public void testBasicAuthorizationHeaderIsSent()
    {
        String response = invoke(
                baseUrl + "/auth/basic",
                "user",
                "secret",
                "basic",
                "GET",
                "",
                "{}",
                false);
        assertEquals("Basic " + Base64.getEncoder().encodeToString("user:secret".getBytes(StandardCharsets.UTF_8)), response);
    }

    @Test
    public void testBearerAuthorizationHeaderIsSent()
    {
        String response = invoke(
                baseUrl + "/auth/bearer",
                "token-123",
                "",
                "bearer",
                "GET",
                "",
                "{}",
                false);
        assertEquals("Bearer token-123", response);
    }

    @Test
    public void testCustomHeadersAreSent()
    {
        String response = invoke(
                baseUrl + "/headers",
                "",
                "",
                "none",
                "GET",
                "",
                "{\"X-Test\":\"alpha\",\"X-Trace\":\"beta\"}",
                false);
        assertEquals("X-Test=alpha;X-Trace=beta", response);
    }

    @Test
    public void testReturnMetaIncludesCookiesHeadersAndBody() throws Exception
    {
        Map<String, Object> response = OBJECT_MAPPER.readValue(
                invoke(baseUrl + "/meta", "", "", "none", "GET", "", "{}", true),
                new TypeReference<>() {});

        assertEquals(200, response.get("status"));
        assertEquals("meta-body", response.get("body"));
        assertEquals(List.of("a=1; Path=/", "b=2; Path=/"), response.get("cookies"));

        @SuppressWarnings("unchecked")
        Map<String, List<String>> headers = (Map<String, List<String>>) response.get("headers");
        assertEquals(List.of("present"), headers.get("x-meta-test"));
    }

    @Test
    public void testRedirectsAreFollowed()
    {
        String response = invoke(baseUrl + "/redirect", "", "", "none", "GET", "", "{}", false);
        assertEquals("redirect-target", response);
    }

    @Test
    public void testConnectionErrorsAreReturned()
            throws IOException
    {
        int unusedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            unusedPort = socket.getLocalPort();
        }

        String response = invoke("http://127.0.0.1:" + unusedPort + "/missing", "", "", "none", "GET", "", "{}", false);
        assertTrue(response.startsWith("ERROR:"), response);
    }

    @Test
    @ResourceLock(Resources.LOCALE)
    public void testMethodNormalizationUsesRootLocale()
    {
        Locale previous = Locale.getDefault();
        Locale.setDefault(Locale.forLanguageTag("tr-TR"));
        try {
            String response = invoke(baseUrl + "/method", "", "", "none", "link", "", "{}", false);
            assertEquals("LINK", response);
        }
        finally {
            Locale.setDefault(previous);
        }
    }

    private String invoke(String url, String user, String password, String authType, String method, String body, String headersJson, boolean returnMeta)
    {
        Slice result = ApiFunctions.callApi(
                Slices.utf8Slice(url),
                Slices.utf8Slice(user),
                Slices.utf8Slice(password),
                Slices.utf8Slice(authType),
                Slices.utf8Slice(method),
                Slices.utf8Slice(headersJson),
                Slices.utf8Slice(body),
                returnMeta);
        return result.toStringUtf8();
    }

    private void basicAuthHandler(HttpExchange exchange) throws IOException
    {
        respond(exchange, exchange.getRequestHeaders().getFirst("Authorization"));
    }

    private void bearerAuthHandler(HttpExchange exchange) throws IOException
    {
        respond(exchange, exchange.getRequestHeaders().getFirst("Authorization"));
    }

    private void headersHandler(HttpExchange exchange) throws IOException
    {
        String payload = "X-Test=" + exchange.getRequestHeaders().getFirst("X-Test") + ";X-Trace=" + exchange.getRequestHeaders().getFirst("X-Trace");
        respond(exchange, payload);
    }

    private void metaHandler(HttpExchange exchange) throws IOException
    {
        exchange.getResponseHeaders().add("Set-Cookie", "a=1; Path=/");
        exchange.getResponseHeaders().add("Set-Cookie", "b=2; Path=/");
        exchange.getResponseHeaders().add("X-Meta-Test", "present");
        respond(exchange, "meta-body");
    }

    private void redirectHandler(HttpExchange exchange) throws IOException
    {
        exchange.getResponseHeaders().add("Location", baseUrl + "/redirect-target");
        exchange.sendResponseHeaders(302, -1);
        exchange.close();
    }

    private void redirectTargetHandler(HttpExchange exchange) throws IOException
    {
        respond(exchange, "redirect-target");
    }

    private void methodHandler(HttpExchange exchange) throws IOException
    {
        respond(exchange, exchange.getRequestMethod());
    }

    private static void respond(HttpExchange exchange, String payload) throws IOException
    {
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
