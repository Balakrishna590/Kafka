package com.bala.kafka.advice;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PracticalAdvice {

	private final String message;
	private final int identifier;

	public PracticalAdvice(@JsonProperty("message") String message, @JsonProperty("identifier") int identifier) {
		super();
		this.message = message;
		this.identifier = identifier;
	}

	public String getMessage() {
		return message;
	}

	public int getIdentifier() {
		return identifier;
	}

	@Override
	public String toString() {
		return "PracticalAdvice [message=" + message + ", identifier=" + identifier + "]";
	}

}
