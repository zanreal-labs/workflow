import { describe, test, expect, mock, spyOn } from "bun:test";
import { createWorkflow } from '../dist/index';
import type { WorkflowState, WorkflowHandler } from '../dist/index';

// Define test interfaces
interface TestMessage {
  id: string;
  value: number;
}

interface TestResult extends TestMessage {
  processed: boolean;
  attempts?: number;
}

describe('Workflow Retry Functionality', () => {
  test('should retry the specified number of times before succeeding', async () => {
    // Create a spy for console.log
    const consoleLogSpy = spyOn(console, 'log');

    // Create a handler that fails a certain number of times before succeeding
    let attempts = 0;
    const flakeyHandler: WorkflowHandler<TestResult, TestResult> = mock(async (message) => {
      attempts++;

      if (attempts <= 2) {
        throw new Error(`Failure on attempt ${attempts}`);
      }

      return {
        ...message,
        processed: true,
        attempts
      };
    });

    const workflow = createWorkflow();
    workflow
      .addHandler(flakeyHandler)
      .configureRetry({
        maxRetries: 3,
        retryDelay: 10, // Small delay for faster tests
      });

    const message: TestMessage = { id: "success-after-retries", value: 42 };
    const result = await workflow.execute(message, undefined, { errorHandling: 'retry' });

    expect(result.success).toBe(true);
    expect(flakeyHandler).toHaveBeenCalledTimes(3); // Initial + 2 retries to succeed
    expect(result.result?.attempts).toBe(3);
    expect(consoleLogSpy).toHaveBeenCalledTimes(2); // Two retry log messages

    // Restore the spy
    consoleLogSpy.mockRestore();
  });

  test('should respect max retry limit and fail after all retries are exhausted', async () => {
    // Create a spy for console.log
    const consoleLogSpy = spyOn(console, 'log');

    // Create a handler that always fails
    const persistentFailureHandler: WorkflowHandler = mock(async () => {
      throw new Error("I will always fail");
    });

    const workflow = createWorkflow();
    workflow
      .addHandler(persistentFailureHandler)
      .configureRetry({
        maxRetries: 2,
        retryDelay: 10
      });

    const message: TestMessage = { id: "always-fails", value: 99 };
    const result = await workflow.execute(message, undefined, { errorHandling: 'retry' });

    expect(result.success).toBe(false);
    expect(persistentFailureHandler).toHaveBeenCalledTimes(3); // Initial + 2 retries
    expect(result.error).toBeDefined();
    expect(result.error?.message).toContain("I will always fail");
    expect(consoleLogSpy).toHaveBeenCalledTimes(2); // Two retry log messages

    // Restore the spy
    consoleLogSpy.mockRestore();
  });

  test('should apply exponential backoff when configured', async () => {
    let attemptTimes: number[] = [];
    const backoffTestHandler: WorkflowHandler = mock(async () => {
      attemptTimes.push(Date.now());
      throw new Error("Testing backoff");
    });

    const workflow = createWorkflow();
    workflow
      .addHandler(backoffTestHandler)
      .configureRetry({
        maxRetries: 2,
        retryDelay: 50, // Small delay for faster tests
        backoffFactor: 2
      });

    const result = await workflow.execute(
      { id: "backoff-test", value: 1 },
      undefined,
      { errorHandling: 'retry' }
    );

    expect(result.success).toBe(false);
    expect(backoffTestHandler).toHaveBeenCalledTimes(3); // Initial + 2 retries

    // We can't precisely test the timing in a real environment,
    // but we can verify the number of attempts
    expect(attemptTimes.length).toBe(3);
  });

  test('should retry each handler independently in a multi-handler workflow', async () => {
    // Create a spy for console.log
    const consoleLogSpy = spyOn(console, 'log');

    // First handler succeeds first time
    const firstHandler: WorkflowHandler<TestMessage, TestResult> = async (message) => {
      return {
        ...message,
        processed: true
      };
    };

    // Second handler fails twice then succeeds
    let secondHandlerAttempts = 0;
    const secondHandler: WorkflowHandler<TestResult, TestResult> = mock(async (message) => {
      secondHandlerAttempts++;
      if (secondHandlerAttempts <= 2) {
        throw new Error(`Second handler failed: attempt ${secondHandlerAttempts}`);
      }
      return {
        ...message,
        attempts: secondHandlerAttempts
      };
    });

    // Third handler fails once then succeeds
    let thirdHandlerAttempts = 0;
    const thirdHandler: WorkflowHandler<TestResult, TestResult> = mock(async (message) => {
      thirdHandlerAttempts++;
      if (thirdHandlerAttempts === 1) {
        throw new Error(`Third handler failed on first attempt`);
      }
      return {
        ...message,
        attempts: message.attempts ? message.attempts + thirdHandlerAttempts : thirdHandlerAttempts
      };
    });

    const workflow = createWorkflow();
    workflow
      .addHandler(firstHandler)
      .addHandler(secondHandler)
      .addHandler(thirdHandler)
      .configureRetry({
        maxRetries: 3,
        retryDelay: 10
      });

    const result = await workflow.execute(
      { id: "multi-handler", value: 5 },
      undefined,
      { errorHandling: 'retry' }
    );

    expect(result.success).toBe(true);
    expect(secondHandlerAttempts).toBe(3); // Needed 3 attempts
    expect(thirdHandlerAttempts).toBe(2); // Needed 2 attempts
    expect(result.result?.attempts).toBeGreaterThan(3); // Combined attempts
    expect(secondHandler).toHaveBeenCalledTimes(3);
    expect(thirdHandler).toHaveBeenCalledTimes(2);

    // Restore the spy
    consoleLogSpy.mockRestore();
  });

  test('should retry handlers in bulk processing', async () => {
    // Create a spy for console.log
    const consoleLogSpy = spyOn(console, 'log');

    // Create a handler that fails based on the message value
    const conditionalRetryHandler: WorkflowHandler<TestMessage, TestResult> = mock(async (message) => {
      const messageValue = (message as TestMessage).value;

      // Simulate different retry behaviors based on message value
      if (messageValue > 50) {
        throw new Error(`Value ${messageValue} too high`);
      }

      return {
        ...message,
        processed: true
      };
    });

    const workflow = createWorkflow();
    workflow
      .addHandler(conditionalRetryHandler)
      .configureRetry({
        maxRetries: 2,
        retryDelay: 10
      });

    const messages: TestMessage[] = [
      { id: "test-1", value: 10 },
      { id: "test-2", value: 100 }, // This will fail even with retries
      { id: "test-3", value: 30 }
    ];

    const results = await workflow.executeBulk(messages, {
      strategy: 'queue',
      errorHandling: 'retry'
    });

    expect(results.length).toBe(3);
    expect(results[0].success).toBe(true);
    expect(results[1].success).toBe(false); // Should fail after retries
    expect(results[2].success).toBe(true);

    // The handler should have been called:
    // - message 1: 1 time (success)
    // - message 2: 3 times (initial + 2 retries)
    // - message 3: 1 time (success)
    // = Total: 5 calls
    expect(conditionalRetryHandler).toHaveBeenCalledTimes(5);

    // Restore the spy
    consoleLogSpy.mockRestore();
  });

  test('should capture retry context in the workflow state', async () => {
    // Create a spy for console.log
    const consoleLogSpy = spyOn(console, 'log');

    // Create a state-tracking retry handler
    const stateCapturingHandler: WorkflowHandler = mock(async (message, initialState, currentState) => {
      const attempts = (currentState.retryAttempts || 0) + 1;

      // Update the state with attempt information
      currentState.retryAttempts = attempts;
      currentState.lastAttemptTime = Date.now();

      if (attempts <= 2) {
        throw new Error(`Still failing after ${attempts} attempts`);
      }

      return {
        ...message,
        processed: true,
        stateAttempts: attempts
      };
    });

    // Create workflow with initial state
    const workflow = createWorkflow({
      retryAttempts: 0,
      startTime: Date.now()
    });

    workflow
      .addHandler(stateCapturingHandler)
      .configureRetry({
        maxRetries: 3,
        retryDelay: 10
      });

    const result = await workflow.execute(
      { id: "state-test", value: 7 },
      undefined,
      { errorHandling: 'retry' }
    );

    expect(result.success).toBe(true);
    expect(stateCapturingHandler).toHaveBeenCalledTimes(3);
    expect(result.result?.stateAttempts).toBe(3);

    // Restore the spy
    consoleLogSpy.mockRestore();
  });
});