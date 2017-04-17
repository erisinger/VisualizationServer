/**
 * Created by erikrisinger on 4/16/17.
 */

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

public class WebSocketServer {

    public static void main(String[] args) throws Exception {
        int port = 8090;
        Server server = new Server(port);

        System.out.println("listening on port " + port);

        WebSocketHandler wsHandler = new WebSocketHandler() {
            @Override
            public void configure(WebSocketServletFactory factory) {
                factory.register(MHLWebSocketHandler.class);
            }
        };

        server.setHandler(wsHandler);
        server.start();
        server.join();
    }
}