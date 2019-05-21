package com.github.nikos.kafka.streams;

import java.io.Serializable;
import java.util.Objects;

public class Topic implements Serializable {
    private String busLine;

    public String getBusLine() {
        return busLine;
    }

    public String getBusLineInput() {
        return "topic-"+busLine+"-input";
    }


    public void setBusLine(String busLine) {
        this.busLine = busLine;
    }

    public Topic(String busLine) {
        this.busLine = busLine;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Topic topic = (Topic) o;
        return busLine.equals(topic.busLine);
    }

    @Override
    public int hashCode() {
        return Objects.hash(busLine);
    }

    @Override
    public String toString() {
        return "Topic{" +
                "busLine='" + busLine + '\'' +
                '}';
    }
}
