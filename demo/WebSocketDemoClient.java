import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Minimal WebSocket subscriber for demonstrating the live-trip gateway, using only the JDK
 * (no dependencies). It authenticates with a bearer token sent as the first frame, subscribes to a
 * topic, and prints every frame the server sends until interrupted.
 *
 * Usage:
 *   javac demo/WebSocketDemoClient.java -d /tmp/wsdemo
 *   java -cp /tmp/wsdemo WebSocketDemoClient ws://localhost:8080/ws/live <JWT> trip:trip-100 [lastSeq]
 */
public class WebSocketDemoClient {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("args: <wsUrl> <token> <topic> [lastSeq] [runSeconds]");
            System.exit(2);
        }
        String url = args[0];
        String token = args[1];
        String topic = args[2];
        long lastSeq = args.length > 3 ? Long.parseLong(args[3]) : 0;
        int runSeconds = args.length > 4 ? Integer.parseInt(args[4]) : 15;

        CountDownLatch done = new CountDownLatch(1);
        HttpClient http = HttpClient.newHttpClient();

        WebSocket ws = http.newWebSocketBuilder()
                .buildAsync(URI.create(url), new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket webSocket) {
                        System.out.println("[open] connected to " + url);
                        // First frame: authenticate (browsers cannot set handshake headers).
                        webSocket.sendText("{\"action\":\"authenticate\",\"token\":\"" + token + "\"}", true);
                        // Then subscribe, resuming from lastSeq.
                        webSocket.sendText("{\"action\":\"subscribe\",\"topic\":\"" + topic
                                + "\",\"lastSeq\":" + lastSeq + "}", true);
                        WebSocket.Listener.super.onOpen(webSocket);
                    }

                    @Override
                    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                        System.out.println("[recv] " + data);
                        return WebSocket.Listener.super.onText(webSocket, data, last);
                    }

                    @Override
                    public void onError(WebSocket webSocket, Throwable error) {
                        System.out.println("[error] " + error.getMessage());
                        done.countDown();
                    }

                    @Override
                    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                        System.out.println("[close] " + statusCode + " " + reason);
                        done.countDown();
                        return null;
                    }
                }).join();

        System.out.println("[info] subscribed to " + topic + "; listening for " + runSeconds + "s");
        done.await(runSeconds, TimeUnit.SECONDS);
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "bye").orTimeout(2, TimeUnit.SECONDS);
        System.out.println("[info] done");
    }
}
