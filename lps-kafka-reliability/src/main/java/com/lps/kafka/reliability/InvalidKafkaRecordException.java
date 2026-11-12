package com.lps.kafka.reliability;

/** Explicitly invalid transport contract; unlike a business conflict, this is safe to isolate. */
public class InvalidKafkaRecordException extends IllegalArgumentException {
    public InvalidKafkaRecordException(String reason) { super(reason); }
}
