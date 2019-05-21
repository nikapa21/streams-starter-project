package com.github.nikos.kafka.streams;

import java.io.Serializable;

public class Message implements Serializable {

	Topic topic;
	Value value;

	public Message(Topic topic, Value value) {
		super();
		this.topic = topic;
		this.value = value;
	}

	public Topic getTopic() {
		return topic;
	}

	public void setTopic(Topic topic) {
		this.topic = topic;
	}

	public Value getValue() {
		return value;
	}

	public void setValue(Value value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "Message{" +
				"topic=" + topic +
				", value=" + value +
				'}';
	}
}
