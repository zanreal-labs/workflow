import { describe, test, expect, mock } from "bun:test";
import { createWorkflow } from '../dist/index';
import type { WorkflowState, WorkflowResult, WorkflowError } from '../dist/index';

// Define test interfaces
interface TestMessage {
  id: string;
  value: number;
}

interface TestResult extends TestMessage {
  processed: boolean;
  multiplied?: number;
  retryAttempts?: number;
}

describe('Workflow Error Handling', () => {
  // Test handlers
  const successHandler = async (message: TestMessage): Promise<TestResult> => {
    return {
      ...message,
      processed: true
    };
  };

  const multiplyHandler = async (message: TestResult): Promise<TestResult> => {
    return {
      ...message,
      multiplied: message.value * 2
    };
  };

  const errorHandler = async (): Promise<TestResult> => {
    throw new Error("Simulated error");
  };

  const conditionalErrorHandler = async (message: TestResult): Promise<TestResult> => {
    if (message.value > 50) {
      throw new Error(`Value ${message.value} exceeds limit`);
    }
    return message;
  };

  test('fail-fast error handling strategy - stops on first error', async () => {
    const workflow = createWorkflow();
    workflow
      .addHandler(successHandler)
      .addHandler(errorHandler)
      .addHandler(multiplyHandler);

    const message: TestMessage = { id: "test-1", value: 10 };
    const result = await workflow.execute(message);

    expect(result.success).toBe(false);
    expect(result.error).toBeDefined();
    if (result.error) {
      expect(result.error.handlerIndex).toBe(1);
      expect(result.error.message).toContain("Simulated error");
    }
    expect(result.result).toBeUndefined();
  });

  test('continue error handling strategy - continues execution after errors', async () => {
    const workflow = createWorkflow();
    workflow
      .addHandler(successHandler)
      .addHandler(errorHandler)
      .addHandler(multiplyHandler);

    const message: TestMessage = { id: "test-1", value: 10 };
    const result = await workflow.execute(message, undefined, { errorHandling: 'continue' });

    expect(result.success).toBe(true);
    expect(result.result).toBeDefined();
    if (result.result) {
      expect(result.result.processed).toBe(true);
      expect(result.result.multiplied).toBe(20); // Shows that the multiply handler ran after the error
    }
  });

  test('retry error handling strategy - retry mechanism works', async () => {
    // Create a mock handler that fails twice then succeeds
    let attempts = 0;
    const flakeyHandler = mock(async (message: TestResult): Promise<TestResult> => {
      attempts++;
      if (attempts <= 2) {
        throw new Error(`Attempt ${attempts} failed`);
      }
      return {
        ...message,
        retryAttempts: attempts
      };
    });

    const workflow = createWorkflow();
    workflow
      .addHandler(successHandler)
      .addHandler(flakeyHandler)
      .configureRetry({
        maxRetries: 3,
        retryDelay: 50,
        backoffFactor: 1.5
      });

    const message: TestMessage = { id: "test-1", value: 10 };
    const result = await workflow.execute(message, undefined, { errorHandling: 'retry' });

    expect(result.success).toBe(true);
    expect(flakeyHandler).toHaveBeenCalledTimes(3); // Called 3 times (2 failures + 1 success)
    expect(result.result?.retryAttempts).toBe(3);
  });

  test('retry error handling strategy - respects max retry limit', async () => {
    // Create a mock handler that always fails
    const alwaysFailHandler = mock(async (): Promise<TestResult> => {
      throw new Error("Always fails");
    });

    const workflow = createWorkflow();
    workflow
      .addHandler(successHandler)
      .addHandler(alwaysFailHandler)
      .configureRetry({
        maxRetries: 2,
        retryDelay: 50
      });

    const message: TestMessage = { id: "test-1", value: 10 };
    const result = await workflow.execute(message, undefined, { errorHandling: 'retry' });

    expect(result.success).toBe(false);
    expect(alwaysFailHandler).toHaveBeenCalledTimes(3); // Initial + 2 retries
    expect(result.error).toBeDefined();
    expect(result.error?.message).toContain("Always fails");
  });

  test('bulk processing with fail-fast stops on first error', async () => {
    const workflow = createWorkflow();
    workflow
      .addHandler(successHandler)
      .addHandler(conditionalErrorHandler);

    const messages: TestMessage[] = [
      { id: "test-1", value: 10 },
      { id: "test-2", value: 100 }, // This will cause an error
      { id: "test-3", value: 30 }
    ];

    const results = await workflow.executeBulk(messages, {
      strategy: 'queue',
      errorHandling: 'fail-fast'
    });

    // We should only have 2 results because we stop on the first error
    expect(results.length).toBe(2);
    expect(results[0].success).toBe(true);
    expect(results[1].success).toBe(false);
    expect(results[1].error?.message).toContain("Value 100 exceeds limit");
  });

  test('bulk processing with continue processes all messages despite errors', async () => {
    const workflow = createWorkflow();
    workflow
      .addHandler(successHandler)
      .addHandler(conditionalErrorHandler);

    const messages: TestMessage[] = [
      { id: "test-1", value: 10 },
      { id: "test-2", value: 100 }, // This will cause an error
      { id: "test-3", value: 30 }
    ];

    const results = await workflow.executeBulk(messages, {
      strategy: 'queue',
      errorHandling: 'continue'
    });

    // We should have 3 results because we continue after errors
    expect(results.length).toBe(3);
    expect(results[0].success).toBe(true);
    expect(results[1].success).toBe(false);
    expect(results[2].success).toBe(true);
  });

  test('parallel execution with error handling', async () => {
    const workflow = createWorkflow();
    workflow
      .addHandler(successHandler)
      .addHandler(conditionalErrorHandler);

    const messages: TestMessage[] = [
      { id: "test-1", value: 10 },
      { id: "test-2", value: 100 }, // This will cause an error
      { id: "test-3", value: 30 }
    ];

    const results = await workflow.executeBulk(messages, {
      strategy: 'parallel',
      errorHandling: 'continue'
    });

    // All messages are processed in parallel
    expect(results.length).toBe(3);
    // Count successes and failures
    const successCount = results.filter(r => r.success).length;
    const failureCount = results.filter(r => !r.success).length;
    expect(successCount).toBe(2);
    expect(failureCount).toBe(1);
  });

  test('bottleneck execution with fail-fast error handling', async () => {
    const workflow = createWorkflow();
    workflow
      .addHandler(successHandler)
      .addHandler(conditionalErrorHandler);

    const messages: TestMessage[] = [
      { id: "test-1", value: 10 },
      { id: "test-2", value: 100 }, // This will cause an error
      { id: "test-3", value: 30 },
      { id: "test-4", value: 40 },
      { id: "test-5", value: 50 }
    ];

    const results = await workflow.executeBulk(messages, {
      strategy: 'bottleneck',
      concurrency: 2,
      errorHandling: 'fail-fast'
    });

    // We should have at least one result (the first one) and one failure
    expect(results.length).toBeGreaterThanOrEqual(1);
    expect(results.some(r => !r.success)).toBe(true);

    // Make sure we have at least one failure
    const errors = results.filter(r => !r.success);
    expect(errors.length).toBeGreaterThanOrEqual(1);
    expect(errors[0].error?.message).toContain("exceeds limit");
  });
});