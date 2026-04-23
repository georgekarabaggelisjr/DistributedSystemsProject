package com.iliasbolan.messaging;

import com.iliasbolan.engine.TaskExecutor;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.CancelCallback; // Added to resolve method ambiguity
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the {@link RabbitMqConsumer} control plane component.
 * <p>
 * This suite validates the worker's ability to consume task definitions from RabbitMQ,
 * coordinate with the computational engine, and handle distributed system edge cases.
 * It has been updated to verify the <b>gRPC-ready signaling</b> through the
 * {@link RabbitMqProducer}.
 * </p>
 * <p>
 * <b>Method Ambiguity Fix:</b><br>
 * Verifications of <code>basicConsume</code> explicitly define the <code>CancelCallback</code>
 * type to distinguish between overloaded method signatures in the RabbitMQ Java client.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.4
 * @since 2026-03-30
 * @see com.iliasbolan.messaging.RabbitMqConsumer
 */
class RabbitMqConsumerTest {

    private RabbitMqConnectionManager mockManager;
    private Connection mockConnection;
    private Channel mockChannel;
    private TaskExecutor mockTaskExecutor;
    private RabbitMqProducer mockEventProducer;

    /**
     * Re-initializes the mocked messaging infrastructure before each test.
     */
    @BeforeEach
    void setUp() throws Exception {
        mockManager = Mockito.mock(RabbitMqConnectionManager.class);
        mockConnection = Mockito.mock(Connection.class);
        mockChannel = Mockito.mock(Channel.class);
        mockTaskExecutor = Mockito.mock(TaskExecutor.class);
        mockEventProducer = Mockito.mock(RabbitMqProducer.class);

        // Wire the mocks to simulate a functional RabbitMQ session
        when(mockManager.createConnection()).thenReturn(mockConnection);
        when(mockConnection.createChannel()).thenReturn(mockChannel);
    }

    /**
     * Verifies that the consumer correctly calculates idle time and triggers
     * a clean pod shutdown once the timeout is exceeded.
     */
    @Test
    void testStartConsuming_IdleTimeout_TriggersGracefulPodShutdown() throws Throwable {
        RabbitMqConsumer consumer = new RabbitMqConsumer(mockManager, "test-queue", 500, mockTaskExecutor, mockEventProducer);

        long startTime = System.currentTimeMillis();

        // Act: Start the blocking loop
        consumer.startConsuming();

        long duration = System.currentTimeMillis() - startTime;

        // Assert: Ensure the loop exited within the configured idle window
        assertTrue(duration >= 500 && duration < 2500,
                "Consumer loop failed to exit within the configured idle timeout window.");
    }

    /**
     * Verifies that successful task execution results in a positive acknowledgment (ACK)
     * and a completion signal through the producer.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testHandleDelivery_SuccessfulTaskExecution_IssuesPositiveAckAndSignal() throws Throwable {
        // Arrange
        RabbitMqConsumer consumer = new RabbitMqConsumer(mockManager, "test-queue", 5000, mockTaskExecutor, mockEventProducer);

        Thread consumerThread = new Thread(() -> {
            try { consumer.startConsuming(); } catch (Exception ignored) {}
        });
        consumerThread.start();
        Thread.sleep(200);

        // Capture the internal deliver callback to simulate a message
        ArgumentCaptor<DeliverCallback> callbackCaptor = ArgumentCaptor.forClass(DeliverCallback.class);

        // FIX: Specified any(CancelCallback.class) to resolve ambiguity
        verify(mockChannel).basicConsume(eq("test-queue"), eq(false), callbackCaptor.capture(), any(CancelCallback.class));

        DeliverCallback internalCallback = callbackCaptor.getValue();

        long deliveryTag = 12345L;
        Envelope envelope = new Envelope(deliveryTag, false, "exchange", "routingKey");
        byte[] body = "{\"jobId\":\"test-job-001\", \"taskId\":\"reduce-0\", \"taskType\":\"REDUCE\"}".getBytes(StandardCharsets.UTF_8);

        // Act: Manually trigger the delivery
        internalCallback.handle("tag", new com.rabbitmq.client.Delivery(envelope, new AMQP.BasicProperties(), body));

        // Assert: Verify engine dispatch and ACK
        verify(mockTaskExecutor).executeTask(anyString());
        verify(mockChannel).basicAck(eq(deliveryTag), eq(false));

        // Assert: Verify signaling via the Producer
        verify(mockEventProducer).sendCompletionSignal(eq("test-job-001"), eq("reduce-0"), eq("COMPLETED"), isNull());

        // Cleanup
        consumer.stopConsuming();
        consumerThread.join();
    }

    /**
     * Verifies that task failures trigger a negative acknowledgment (NACK) with requeue
     * and broadcast a FAILED event via the producer.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testHandleDelivery_TaskExecutionFailure_IssuesNackAndFailedSignal() throws Throwable {
        // Arrange
        RabbitMqConsumer consumer = new RabbitMqConsumer(mockManager, "test-queue", 5000, mockTaskExecutor, mockEventProducer);
        doThrow(new RuntimeException("I/O Error")).when(mockTaskExecutor).executeTask(anyString());

        Thread consumerThread = new Thread(() -> {
            try { consumer.startConsuming(); } catch (Exception ignored) {}
        });
        consumerThread.start();
        Thread.sleep(200);

        ArgumentCaptor<DeliverCallback> callbackCaptor = ArgumentCaptor.forClass(DeliverCallback.class);

        // FIX: Specified any(CancelCallback.class) to resolve ambiguity
        verify(mockChannel).basicConsume(eq("test-queue"), eq(false), callbackCaptor.capture(), any(CancelCallback.class));

        DeliverCallback internalCallback = callbackCaptor.getValue();

        long deliveryTag = 9999L;
        Envelope envelope = new Envelope(deliveryTag, false, "exchange", "routingKey");
        byte[] body = "{\"jobId\":\"test-job-002\", \"taskId\":\"map-1\"}".getBytes(StandardCharsets.UTF_8);

        // Act
        internalCallback.handle("tag", new com.rabbitmq.client.Delivery(envelope, new AMQP.BasicProperties(), body));

        // Assert: Verify NACK with requeue=true
        verify(mockChannel).basicNack(eq(deliveryTag), eq(false), eq(true));

        // Assert: Verify the failure was signaled through the producer
        verify(mockEventProducer).sendCompletionSignal(eq("test-job-002"), eq("map-1"), eq("FAILED"), anyString());

        consumer.stopConsuming();
        consumerThread.join();
    }

    /**
     * Validates the Poison Pill protection mechanism.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testHandleDelivery_MaxRetriesExceeded_SignalsFailureAndDropsMessage() throws Throwable {
        // Arrange
        RabbitMqConsumer consumer = new RabbitMqConsumer(mockManager, "test-queue", 5000, mockTaskExecutor, mockEventProducer);

        Thread consumerThread = new Thread(() -> {
            try { consumer.startConsuming(); } catch (Exception ignored) {}
        });
        consumerThread.start();
        Thread.sleep(200);

        ArgumentCaptor<DeliverCallback> callbackCaptor = ArgumentCaptor.forClass(DeliverCallback.class);

        // FIX: Specified any(CancelCallback.class) to resolve ambiguity
        verify(mockChannel).basicConsume(eq("test-queue"), eq(false), callbackCaptor.capture(), any(CancelCallback.class));

        DeliverCallback internalCallback = callbackCaptor.getValue();

        long deliveryTag = 8888L;
        Envelope envelope = new Envelope(deliveryTag, false, "exchange", "routingKey");
        byte[] body = "{\"jobId\":\"poison-pill\", \"taskId\":\"map-2\"}".getBytes(StandardCharsets.UTF_8);

        java.util.Map<String, Object> headers = new java.util.HashMap<>();
        headers.put("x-delivery-count", 5);
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().headers(headers).build();

        // Act
        internalCallback.handle("tag", new com.rabbitmq.client.Delivery(envelope, properties, body));

        // Assert 1: Ensure message is NACKed with requeue=false
        verify(mockChannel).basicNack(eq(deliveryTag), eq(false), eq(false));

        // Assert 2: Verify signaled via Producer
        verify(mockEventProducer).sendCompletionSignal(
                eq("poison-pill"),
                eq("map-2"),
                eq("FAILED"),
                eq("MAX_RETRIES_EXCEEDED")
        );

        // Assert 3: Computational engine must be bypassed
        verify(mockTaskExecutor, never()).executeTask(anyString());

        consumer.stopConsuming();
        consumerThread.join();
    }
}