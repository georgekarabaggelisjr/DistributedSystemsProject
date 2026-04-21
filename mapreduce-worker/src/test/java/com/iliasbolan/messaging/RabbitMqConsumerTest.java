package com.iliasbolan.messaging;

import com.iliasbolan.engine.TaskExecutor;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link RabbitMqConsumer}.
 * This verifies the interaction with the RabbitMQ driver, including graceful
 * timeouts, manual acknowledgments (ACK), and error re-queuing (NACK).
 * <p>
 * <b>Note:</b> These tests are updated to handle {@link Throwable} signatures
 * originating from the Resilience4j-wrapped TaskExecutor.
 * </p>
 */
class RabbitMqConsumerTest {

    private RabbitMqConnectionManager mockManager;
    private Connection mockConnection;
    private Channel mockChannel;
    private TaskExecutor mockTaskExecutor;

    @BeforeEach
    void setUp() throws Exception {
        mockManager = Mockito.mock(RabbitMqConnectionManager.class);
        mockConnection = Mockito.mock(Connection.class);
        mockChannel = Mockito.mock(Channel.class);
        mockTaskExecutor = Mockito.mock(TaskExecutor.class);

        // Wire the mocks together so the consumer thinks it has a real network
        when(mockManager.createConnection()).thenReturn(mockConnection);
        when(mockConnection.createChannel()).thenReturn(mockChannel);
    }

    /**
     * Verifies that the consumer correctly calculates its idle time and triggers
     * a clean shutdown of the main thread once the timeout is exceeded.
     * * @throws Throwable to accommodate the TaskExecutor's updated signatures.
     */
    @Test
    void testStartConsuming_IdleTimeout_TriggersCleanShutdown() throws Throwable {
        // Arrange: Set a very short timeout of 500ms
        RabbitMqConsumer consumer = new RabbitMqConsumer(mockManager, "test-queue", 500, mockTaskExecutor);

        long startTime = System.currentTimeMillis();

        // Act: Start consuming. Because we don't send any messages, this should block
        // for exactly 500ms and then exit cleanly thanks to our stopConsuming() fix!
        consumer.startConsuming();

        long duration = System.currentTimeMillis() - startTime;

        // Assert: Ensure it waited at least 500ms, but didn't hang forever
        assertTrue(duration >= 500 && duration < 2000,
                "Consumer did not shut down within the expected idle timeout window!");
    }

    /**
     * Ensures that upon successful task completion, the consumer issues a
     * positive acknowledgment (ACK) to the broker.
     * * @throws Throwable to accommodate the TaskExecutor's updated signatures.
     */
    @Test
    void testHandleDelivery_SuccessfulTask_SendsAck() throws Throwable {
        // Arrange
        RabbitMqConsumer consumer = new RabbitMqConsumer(mockManager, "test-queue", 5000, mockTaskExecutor);

        // Run startConsuming in a separate thread so it doesn't block our test
        Thread consumerThread = new Thread(() -> {
            try { consumer.startConsuming(); } catch (Exception ignored) {}
        });
        consumerThread.start();

        // Give it 100ms to initialize and attach the RabbitMQ Consumer object to our mock channel
        Thread.sleep(100);

        // Capture the internal RabbitMQ Consumer object so we can manually trigger a message delivery
        ArgumentCaptor<Consumer> consumerCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockChannel).basicConsume(eq("test-queue"), eq(false), consumerCaptor.capture());
        Consumer internalRabbitConsumer = consumerCaptor.getValue();

        // Simulate a message arriving from RabbitMQ
        long deliveryTag = 12345L;
        Envelope envelope = new Envelope(deliveryTag, false, "exchange", "routingKey");
        byte[] body = "{\"jobId\":\"1\"}".getBytes(StandardCharsets.UTF_8);

        // Act: Manually fire the delivery event
        internalRabbitConsumer.handleDelivery("tag", envelope, new AMQP.BasicProperties(), body);

        // Assert: Verify it executed the task and sent a positive ACK
        // verify() must account for executeTask now throwing Throwable
        verify(mockTaskExecutor).executeTask(anyString());
        verify(mockChannel).basicAck(eq(deliveryTag), eq(false));

        // Clean up
        consumer.stopConsuming();
        consumerThread.join();
    }

    /**
     * Verifies that if a task fails or a Throwable is caught, the consumer
     * issues a negative acknowledgment (NACK) with requeue=true.
     * * @throws Throwable to accommodate the TaskExecutor's updated signatures.
     */
    @Test
    void testHandleDelivery_TaskFails_SendsNack() throws Throwable {
        // Arrange
        RabbitMqConsumer consumer = new RabbitMqConsumer(mockManager, "test-queue", 5000, mockTaskExecutor);

        // Make the task executor throw a simulated exception
        // doThrow handles Throwable appropriately here
        doThrow(new RuntimeException("Simulated processing crash!")).when(mockTaskExecutor).executeTask(anyString());

        Thread consumerThread = new Thread(() -> {
            try { consumer.startConsuming(); } catch (Exception ignored) {}
        });
        consumerThread.start();
        Thread.sleep(100);

        ArgumentCaptor<Consumer> consumerCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockChannel).basicConsume(eq("test-queue"), eq(false), consumerCaptor.capture());
        Consumer internalRabbitConsumer = consumerCaptor.getValue();

        long deliveryTag = 9999L;
        Envelope envelope = new Envelope(deliveryTag, false, "exchange", "routingKey");

        // Act
        internalRabbitConsumer.handleDelivery("tag", envelope, new AMQP.BasicProperties(), "bad-payload".getBytes());

        // Assert: Verify it sent a negative NACK with 'requeue = true' so the message isn't lost
        verify(mockChannel).basicNack(eq(deliveryTag), eq(false), eq(true));

        consumer.stopConsuming();
        consumerThread.join();
    }

    /**
     * Verifies the Poison Pill protection mechanism. If a task exceeds the MAX_RETRIES
     * threshold, it should be NACKed with requeue=false (sending it to the DLX)
     * and a FAILED status event should be broadcasted to the Manager.
     * * @throws Throwable to accommodate the TaskExecutor's updated signatures.
     */
    @Test
    void testHandleDelivery_PoisonPill_SendsNackWithoutRequeue() throws Throwable {
        // Arrange
        RabbitMqConsumer consumer = new RabbitMqConsumer(mockManager, "test-queue", 5000, mockTaskExecutor);

        Thread consumerThread = new Thread(() -> {
            try { consumer.startConsuming(); } catch (Exception ignored) {}
        });
        consumerThread.start();
        Thread.sleep(100);

        ArgumentCaptor<Consumer> consumerCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockChannel).basicConsume(eq("test-queue"), eq(false), consumerCaptor.capture());
        Consumer internalRabbitConsumer = consumerCaptor.getValue();

        long deliveryTag = 8888L;
        Envelope envelope = new Envelope(deliveryTag, false, "exchange", "routingKey");
        byte[] body = "{\"jobId\":\"poison-job\", \"taskId\":\"map-1\"}".getBytes(StandardCharsets.UTF_8);

        // Simulate x-delivery-count header = 4 (Exceeding MAX_RETRIES = 3)
        java.util.Map<String, Object> headers = new java.util.HashMap<>();
        headers.put("x-delivery-count", 4);
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .headers(headers)
                .build();

        // Act
        internalRabbitConsumer.handleDelivery("tag", envelope, properties, body);

        // Assert: Ensure it was NACKed with requeue=FALSE (to be routed to DLX)
        verify(mockChannel).basicNack(eq(deliveryTag), eq(false), eq(false));

        // Assert: Ensure a FAILED event was published back to the status queue
        ArgumentCaptor<byte[]> publishCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(mockChannel).basicPublish(eq(""), eq("job_events_queue"), isNull(), publishCaptor.capture());
        String publishedEvent = new String(publishCaptor.getValue(), StandardCharsets.UTF_8);
        assertTrue(publishedEvent.contains("\"state\": \"FAILED\""));
        assertTrue(publishedEvent.contains("MAX_RETRIES_EXCEEDED"));

        // Assert: Ensure the TaskExecutor was completely bypassed for poison pills
        verify(mockTaskExecutor, Mockito.never()).executeTask(anyString());

        consumer.stopConsuming();
        consumerThread.join();
    }
}