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
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
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
        String urlString = apiUrl.toStringUtf8();
        String user = usernameOrToken.toStringUtf8();
        String pass = password.toStringUtf8();
        String auth = authType.toStringUtf8();
        String httpMethod = method.toStringUtf8().toUpperCase();
        String headersRaw = headersJson.toStringUtf8();
        String bodyContent = requestBody.toStringUtf8();

        String cacheKey = urlString + "|" + user + "|" + pass + "|" + auth + "|" + httpMethod + "|" + headersRaw + "|" + bodyContent;
        if (CACHE.containsKey(cacheKey)) {
            return Slices.utf8Slice(CACHE.get(cacheKey));
        }

        try {
            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(httpMethod);

            // Authorization
            if (auth.equalsIgnoreCase("basic")) {
                String credentials = user + ":" + pass;
                String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
                conn.setRequestProperty("Authorization", "Basic " + encoded);
            } else if (auth.equalsIgnoreCase("bearer")) {
                conn.setRequestProperty("Authorization", "Bearer " + user);
            }

            // Headers
            if (!headersRaw.isEmpty()) {
                Map<String, String> headers = OBJECT_MAPPER.readValue(headersRaw, new TypeReference<Map<String, String>>() {});
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    conn.setRequestProperty(entry.getKey(), entry.getValue());
                }
            }

            // POST or PUT body
            if (httpMethod.equals("POST") || httpMethod.equals("PUT")) {
                conn.setDoOutput(true);
                byte[] inputBytes = bodyContent.getBytes(StandardCharsets.UTF_8);
                conn.setRequestProperty("Content-Length", Integer.toString(inputBytes.length));
                try (OutputStream os = conn.getOutputStream()) {
                    os.write(inputBytes);
                }
            }

            int responseCode = conn.getResponseCode();
            BufferedReader reader;
            if (responseCode >= 200 && responseCode < 300) {
                reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
            } else {
                reader = new BufferedReader(new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8));
            }

            StringBuilder response = new StringBuilder();
            String inputLine;
            while ((inputLine = reader.readLine()) != null) {
                response.append(inputLine);
            }
            reader.close();
            conn.disconnect();

            String result = (responseCode >= 200 && responseCode < 300) ? response.toString() : "HTTP_ERROR: " + responseCode + " BODY: " + response.toString();
            CACHE.put(cacheKey, result);

            return Slices.utf8Slice(result);
        } catch (Exception e) {
            return Slices.utf8Slice("ERROR: " + e.getMessage());
        }
    }
}
