package com.example.trino.functions;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;

public class ApiFunctions {
    private static final ConcurrentHashMap<String, String> CACHE = new ConcurrentHashMap<>();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Description("Calls a REST API with optional auth, method, headers, body; returns response as string")
    @ScalarFunction("call_api")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice callApi(
            @SqlType(StandardTypes.VARCHAR) Slice apiUrl,
            @SqlType(StandardTypes.VARCHAR) Slice usernameOrToken,
            @SqlType(StandardTypes.VARCHAR) Slice password,
            @SqlType(StandardTypes.VARCHAR) Slice authType,
            @SqlType(StandardTypes.VARCHAR) Slice method,
            @SqlType(StandardTypes.VARCHAR) Slice headersJson,
            @SqlType(StandardTypes.VARCHAR) Slice requestBody
    ) {
        return callApi(apiUrl, usernameOrToken, password, authType, method, headersJson, requestBody, false);
    }

    @Description("Calls a REST API; if return_meta is true, returns JSON with status, headers, cookies, body; otherwise returns response body")
    @ScalarFunction("call_api")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice callApi(
            @SqlType(StandardTypes.VARCHAR) Slice apiUrl,
            @SqlType(StandardTypes.VARCHAR) Slice usernameOrToken,
            @SqlType(StandardTypes.VARCHAR) Slice password,
            @SqlType(StandardTypes.VARCHAR) Slice authType,
            @SqlType(StandardTypes.VARCHAR) Slice method,
            @SqlType(StandardTypes.VARCHAR) Slice headersJson,
            @SqlType(StandardTypes.VARCHAR) Slice requestBody,
            @SqlType(StandardTypes.BOOLEAN) boolean returnMeta
    ) {
        String urlString = apiUrl.toStringUtf8();
        String user = usernameOrToken.toStringUtf8();
        String pass = password.toStringUtf8();
        String auth = authType.toStringUtf8();
        String httpMethod = method.toStringUtf8().toUpperCase();
        String headersRaw = headersJson.toStringUtf8();
        String bodyContent = requestBody.toStringUtf8();

        String cacheKey = urlString + "|" + user + "|" + pass + "|" + auth + "|" + httpMethod + "|" + headersRaw + "|" + bodyContent + "|" + returnMeta;
        if (CACHE.containsKey(cacheKey)) {
            return Slices.utf8Slice(CACHE.get(cacheKey));
        }

        try {
            Response response = executeRequest(
                    urlString,
                    user,
                    pass,
                    auth,
                    httpMethod,
                    headersRaw,
                    bodyContent
            );

            String result;
            if (returnMeta) {
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("status", response.status);
                payload.put("headers", response.headers);
                payload.put("cookies", response.cookies);
                payload.put("body", response.body);
                result = OBJECT_MAPPER.writeValueAsString(payload);
            } else {
                result = (response.status >= 200 && response.status < 300)
                        ? response.body
                        : "HTTP_ERROR: " + response.status + " BODY: " + response.body;
            }
            CACHE.put(cacheKey, result);

            return Slices.utf8Slice(result);
        } catch (Exception e) {
            return Slices.utf8Slice("ERROR: " + e.getMessage());
        }
    }

    private static Response executeRequest(
            String urlString,
            String user,
            String pass,
            String auth,
            String httpMethod,
            String headersRaw,
            String bodyContent
    ) throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.noBody();
        if (httpMethod.equals("POST") || httpMethod.equals("PUT")) {
            bodyPublisher = HttpRequest.BodyPublishers.ofString(bodyContent, StandardCharsets.UTF_8);
        }

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(urlString))
                .method(httpMethod, bodyPublisher);

        // Authorization
        if (auth.equalsIgnoreCase("basic")) {
            String credentials = user + ":" + pass;
            String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
            requestBuilder.header("Authorization", "Basic " + encoded);
        } else if (auth.equalsIgnoreCase("bearer")) {
            requestBuilder.header("Authorization", "Bearer " + user);
        }

        // Headers
        if (!headersRaw.isEmpty()) {
            Map<String, String> headers = OBJECT_MAPPER.readValue(headersRaw, new TypeReference<Map<String, String>>() {});
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                requestBuilder.header(entry.getKey(), entry.getValue());
            }
        }

        HttpResponse<String> response = client.send(
                requestBuilder.build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
        );

        Map<String, List<String>> headers = new LinkedHashMap<>(response.headers().map());

        List<String> cookies = headers.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getKey().equalsIgnoreCase("Set-Cookie"))
                .flatMap(entry -> entry.getValue().stream())
                .collect(Collectors.toList());

        return new Response(response.statusCode(), response.body(), headers, cookies);
    }

    private static class Response {
        private final int status;
        private final String body;
        private final Map<String, List<String>> headers;
        private final List<String> cookies;

        private Response(int status, String body, Map<String, List<String>> headers, List<String> cookies) {
            this.status = status;
            this.body = body;
            this.headers = headers;
            this.cookies = cookies;
        }
    }
}
