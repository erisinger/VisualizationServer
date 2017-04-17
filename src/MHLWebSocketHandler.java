/**
 * Created by erikrisinger on 4/16/17.
 */
import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;

import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;

@WebSocket
public class MHLWebSocketHandler {
    private Session session;
    private KafkaConsumer<String, String> consumer;
    private String id = "empty";
    private boolean socketOpen = false;
    Thread streamThread;

    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        socketOpen = false;
        streamThread.interrupt();
        System.out.print("websocket closed...");
//        System.out.println("Close: statusCode=" + statusCode + ", reason=" + reason);
    }

    @OnWebSocketError
    public void onError(Throwable t) {
//        System.out.println("Error: " + t.getMessage());
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
        socketOpen = true;
        this.session = session;
        System.out.print("connected to " + session.getRemoteAddress().getAddress());
    }

    @OnWebSocketMessage
    public void onMessage(String message) {
        if (message == null) return;

        Properties props = new Properties();

        String[] split = message.split(",");
        if (split.length == 2 && "ID".equals(split[0])){
            id = split[1];
            //kafka consumer code
//            props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
            props.put("bootstrap.servers", "none.cs.umass.edu:9092,none.cs.umass.edu:9093,none.cs.umass.edu:9094");
            props.put("group.id", "VS" + System.currentTimeMillis());
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "30000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList("data-message", "analytics-message"));

            System.out.println("...streaming data for ID " + id);

            streamThread = new Thread(new DataStreamerRunnable(consumer, this.session.getRemote(), id));
            streamThread.start();

        } else {
            System.out.println("aborting on failed handshake " + message);
        }
    }

    private class DataStreamerRunnable implements Runnable {
        KafkaConsumer<String, String> cons;
        RemoteEndpoint endpoint;
        String id;
        JSONParser parser = new JSONParser();

        public DataStreamerRunnable(KafkaConsumer<String, String> c, RemoteEndpoint e, String id) {
            this.cons = c;
            this.endpoint = e;
            this.id = id;
        }

        @Override
        public void run() {
            try {
                JSONObject obj;
                JSONObject header;
                String recordID;

                while (!Thread.currentThread().isInterrupted()) {

                    if (Thread.currentThread().isInterrupted()) break;

                    ConsumerRecords<String, String> records = consumer.poll(2000);
                    for (ConsumerRecord<String, String> record : records) {

                        try {
                            obj = (JSONObject)parser.parse(record.value());
                            header = (JSONObject)obj.get("header");
                            recordID = (String)header.get("badge-id");

                            if (recordID.equals(this.id)) {
                                endpoint.sendString(record.value());
                            }
                        } catch (ParseException p) {
                            System.out.println("parsing failed for string " + record.value());
                        }
                    }

                }
            } catch (IOException e) {

            } finally {
                cons.close();
                System.out.println("consumer closed...session ended for " + session.getRemoteAddress().getAddress());
            }
        }
    }
}
