package handlers;

import java.util.HashMap;

public class MessageHandlerRegistry {
    private static final HashMap<String, KafkaMessageHandler> handlerMap;

    static {
        handlerMap = new HashMap<>();
        handlerMap.put("Fitbit", new FitbitMessageHandler());
        handlerMap.put("Scale", new ScaleMessageHandler());
        handlerMap.put("Location", new LocationMessageHandler());
        handlerMap.put("Terminate", new TerminateMessageHandler());
        handlerMap.put("ABC", new ABCMessageHandler());
    }

    public static KafkaMessageHandler getHandler(String messageType) {
        return handlerMap.get(messageType);
    }
}