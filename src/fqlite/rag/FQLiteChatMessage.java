package fqlite.rag;

import java.time.LocalTime;

/**
 * Chat Message class. Just for internal handling.
 *
 * @author pawlaszc
 */
class FQLiteChatMessage {
    String sender;
    String message;
    LocalTime timestamp;

    FQLiteChatMessage(String sender, String message) {
        this.sender = sender;
        this.message = message;
        this.timestamp = LocalTime.now();
    }
}
