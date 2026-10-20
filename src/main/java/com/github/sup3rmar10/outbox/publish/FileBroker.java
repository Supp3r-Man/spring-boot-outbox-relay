package com.github.sup3rmar10.outbox.publish;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Message;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.PublishException;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Publisher;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/** Appends one JSON object per line. Stands in for "a broker" you can inspect with a text editor. Not fsynced. */
public class FileBroker implements Publisher {

    private final Path file;
    private final ObjectMapper json;

    public FileBroker(Path file, ObjectMapper json) {
        this.file = file;
        this.json = json;
    }

    @Override
    public synchronized void publish(Message message) throws PublishException {
        try {
            String line = json.writeValueAsString(message) + "\n";
            Files.writeString(file, line, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new PublishException("cannot append to " + file, e);
        }
    }

    public synchronized List<Message> readAll() throws IOException {
        List<Message> out = new ArrayList<>();
        if (!Files.exists(file)) {
            return out;
        }
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                out.add(json.readValue(line, Message.class));
            }
        }
        return out;
    }
}
