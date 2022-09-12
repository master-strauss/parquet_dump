[![Java CI with Gradle](https://github.com/MarcoLotz/parquet_dump/actions/workflows/gradle.yml/badge.svg)](https://github.com/MarcoLotz/parquet_dump/actions/workflows/gradle.yml)

# Parque dump

Run parallel SQL statements on to a database, convert to Parquet, optionally encrypt and write the content to disk.

## Overview

## Implementation decisions

This is my favourite part :)

### Sequential processing

This application is a sequence of steps:
1. Fetch Data from DB
2. Convert Data to Parquet
3. Encrypt Data
4. Write data to the file system

An observer with a software engineering background will be able to spot that:
* Step (1) and (4) are I/O Bound. Step 1 is Network I/O bound and Step 4 is Disk Write I/O bound.
* Step (2) and (3) are CPU Bound. Both steps, however, are only as fast as their Big(O) implementation.

From all the steps, it's easy to see that the Step (1) should be the slowest.

A naive implementation could apply all these steps in sequence:

* t0: Fetch data
* t1: Convert Data to Parquet
* t2: Encrypt Data
* t3: Write data to FS

This means that no fetch will be performed for a t3-t0 period, which results in idle network time.
Time that could be used to be retrieving more data from the DB is being used for CPU bounded operations and disk write I/O.
This is solved by:

### Introducing Disruptor

In order to minimize the intervals between network fetches, one could easily delegate the data, as soon as it is collected, to other threads to process.
The solution for this is a Queue like data structure between the multiple steps of the process.
There are multiple queue implementations, including concurrent ones.

I have decided to use Disruptor in this because it maximizes cache usage: The messages are usually in either L2/L3 caches - which greatly improves the speed multiple threads are accessing it some the same core or same die.
In parallel to that to deal with the write contention, a queue often uses locks, which can cause a context switch to the kernel.
When this happens the processor involved is likely to lose the data in its caches.
This doesn't happen for disruptor.

Disruptor has a few other nice features, like an excellent implementation for multiple producer-multiple consumer queues.
In our case, we only used one consumer one producer implementation.

### Interface Segregation

I have used Hexagonal Architecture for this project.
There multiple ports, declaring interfaces for the main components of the software artifact.
The implementation of those components is done on the core package.
I personally do not like to use the convention InterfaceNameImpl naming to implementation classes.
Since Java libraries dont use that (there's no ListImpl but a specification detail of the implementation e.g. ArrayList).
I used the same criteria when implementing the interfaces.

### Dependency Injection

Due to the Interface Segregation, one can inject any implementation of the ports and it should work out of the box.
One should be able to configure which bean to use (e.g. AES-256 encryption or no encryption are just a bean away).
I have so far, however, no implemented this kind of configuration even thought the implementations do exist.
The reason is that this configuration is not required for the MVP.
I tested to make sure that Liskov's Substitution Principle holds on all cases.

### Synchronized

As I implemented, the ingestions should be thread-safe.
I have performed so by adding @Synchronized to the ingestions service.
Of course, other strategies could be used here (e.g. Using a concurrent data structure) - but this is not required for the MVP.
Ideally soon we should be able to receive a "BUSY" status code when another ingestion is currently happening.
This, however, is still to be implemented.

### Single Responsibility

I tried to dissociated the data processing part (e.g. encryption) to the data transmission part (encryptionTransformer).
That's why there are many "product", "consumer" and "transformer" implementations.
In reality, they all have similar code that could be simplified to certain abstract implementations.
This is something to be added soon(tm).

### Async Logging

Just for fun I used async / lazy logging (Log4j2).
I've been working with lots of Kafka lately and Async logging really makes a difference on high-throughput systems.
Since this is a system that we are designing just to have high thought-put to filesystem, I want to reduce the dependency on logging FS write time.

### OpenAPI Contracts
API definitions are contracts between consumer and producers.
With this in mind and to avoid push-pull conflicts between both sides, it's a good practice to handle API as a document, version it, and auto-generate the code for it.
In this project, I used OpenAPI 3.0 and auto-generated the code of all controllers from it.
The API contract is available [here](src/main/resources/command_ingestion.yml).

### Spring Boot
Spring (Boot) is one of the most popular java frameworks for dependency injection.
With that in mind, all my auto-generated code snippets are Spring Controllers.
Aside from controllers and configurations, I never use @Component annotations - and there's a reason for that.
Whenever creating a Spring Bean with @Configuration (e.g. a Service bean from a @Configuration file) it is a lot easier to segregate and load only the required beans for a given Integration Test.
IT tests in Spring are usually expensive, since they require lots of context loading.
Reducing the number of beans to be loaded greatly improves the speed of those tests and reduces the testing interface.

### Completable Futures as Controller return data type
Servlet container threads are really expensive, and they are a scarce resource.
Holding a container thread for long periods of time greatly reduces the capabilities of your system to scale-up.
With this in mind, all my controllers return CompletableFutures.
The endpoints implement similar logic too, where they log something in debug level and then spin off the heavy processing into another thread.
Since I use async logging, even the logging is performed in another thread.
Thus a container thread is only allocated for about the same amount of time required to spin the working thread to handle the controller request.