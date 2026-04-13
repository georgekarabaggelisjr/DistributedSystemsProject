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

    @Test
    void testStartConsuming_IdleTimeout_TriggersCleanShutdown() throws Exception {
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

    @Test
    void testHandleDelivery_SuccessfulTask_SendsAck() throws Exception {
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
        verify(mockTaskExecutor).executeTask(anyString());
        verify(mockChannel).basicAck(eq(deliveryTag), eq(false));

        // Clean up
        consumer.stopConsuming();
        consumerThread.join();
    }

    @Test
    void testHandleDelivery_TaskFails_SendsNack() throws Exception {
        // Arrange
        RabbitMqConsumer consumer = new RabbitMqConsumer(mockManager, "test-queue", 5000, mockTaskExecutor);

        // Make the task executor throw a simulated exception
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
}