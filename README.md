# Kafka-system-design

This is a practice kafka system design repo.

## The Problem

Design the following system:

a kafka producer that produces 1000 messages into a kafka topic as a batch. the producer produces massages that have differnt states, some states are failed, some are completed and some are in progress.
write a kafka consumer the reads data from this topic and parses these states and store them into a persistent store.
the persistent store now has various messages that are either successful, in progress or failed.
write a test suite that is able to assert that there was no message dropped and the message were correctly parsed into each bucket of successful, in progress or failed.
the system is not statis but is a streaming system.