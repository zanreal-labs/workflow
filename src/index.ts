// Types and interfaces
export type WorkflowState = Record<string, any>;
export type WorkflowHandler<T = any, R = any> = (message: T, initialState: WorkflowState, currentState: WorkflowState) => Promise<R>;

export interface WorkflowExecutionOptions {
  strategy: 'queue' | 'parallel' | 'bottleneck';
  concurrency?: number; // For bottleneck strategy
  errorHandling?: ErrorHandlingStrategy;
}

export type ErrorHandlingStrategy = 'fail-fast' | 'continue' | 'retry';

export interface RetryOptions {
  maxRetries: number;
  retryDelay: number;
  backoffFactor?: number;
}

export interface WorkflowError<T = any> {
  message: string;
  originalError: Error;
  handlerIndex: number;
  inputMessage: T;
  state: WorkflowState;
}

export interface WorkflowResult<R = any> {
  success: boolean;
  result?: R;
  error?: WorkflowError;
}

/**
 * Workflow class for processing messages through a series of handlers
 */
export class Workflow<T = any, R = any> {
  private handlers: WorkflowHandler<T, R>[] = [];
  private initialState: WorkflowState = {};
  private retryOptions: RetryOptions = {
    maxRetries: 3,
    retryDelay: 500,
    backoffFactor: 2
  };

  /**
   * Create a new Workflow instance
   * @param initialState Optional initial state for the workflow
   */
  constructor(initialState: WorkflowState = {}) {
    this.initialState = { ...initialState };
  }

  /**
   * Add a handler function to the workflow
   * @param handler The handler function to add
   * @returns The workflow instance for chaining
   */
  addHandler(handler: WorkflowHandler<T, R>): Workflow<T, R> {
    this.handlers.push(handler);
    return this;
  }

  /**
   * Configure retry options for the workflow
   * @param options Retry configuration options
   * @returns The workflow instance for chaining
   */
  configureRetry(options: Partial<RetryOptions>): Workflow<T, R> {
    this.retryOptions = { ...this.retryOptions, ...options };
    return this;
  }

  /**
   * Execute the workflow with a single message
   * @param message The message to process
   * @param currentState Optional current state (defaults to initialState)
   * @param options Optional execution options
   * @returns Result of the workflow execution
   */
  async execute(
    message: T,
    currentState?: WorkflowState,
    options?: { errorHandling?: ErrorHandlingStrategy }
  ): Promise<WorkflowResult<R>> {
    const state = currentState ? { ...this.initialState, ...currentState } : { ...this.initialState };
    let result: any = message;
    const errorHandling = options?.errorHandling || 'fail-fast';
    let lastError: WorkflowError<T> | undefined = undefined;

    try {
      for (let i = 0; i < this.handlers.length; i++) {
        const handler = this.handlers[i]!;
        try {
          result = await this.executeHandlerWithRetry(handler, result, i, state, errorHandling);
        } catch (error) {
          const workflowError: WorkflowError<T> = {
            message: `Error in handler at index ${i}: ${error instanceof Error ? error.message : String(error)}`,
            originalError: error instanceof Error ? error : new Error(String(error)),
            handlerIndex: i,
            inputMessage: message as T,
            state: { ...state }
          };

          // Store the last error that occurred
          lastError = workflowError;

          if (errorHandling === 'continue') {
            // Log the error but continue with the next handler
            console.error(workflowError.message);
            continue;
          } else {
            // For 'fail-fast' or if retries failed
            return {
              success: false,
              error: workflowError
            };
          }
        }
      }

      // If we had errors but used 'continue', return the error in the result
      if (lastError && errorHandling === 'continue') {
        return {
          success: false,
          error: lastError,
          // Still include the result as we've processed it partially
          result: result as R
        };
      }

      return {
        success: true,
        result: result as R
      };
    } catch (error) {
      const workflowError: WorkflowError<T> = {
        message: `Unhandled workflow error: ${error instanceof Error ? error.message : String(error)}`,
        originalError: error instanceof Error ? error : new Error(String(error)),
        handlerIndex: -1,
        inputMessage: message as T,
        state: { ...state }
      };

      return {
        success: false,
        error: workflowError
      };
    }
  }

  /**
   * Execute a handler with retry logic if configured
   * @private
   */
  private async executeHandlerWithRetry(
    handler: WorkflowHandler<T, R>,
    message: any,
    handlerIndex: number,
    state: WorkflowState,
    errorHandling: ErrorHandlingStrategy
  ): Promise<any> {
    if (errorHandling !== 'retry') {
      return await handler(message, this.initialState, state);
    }

    let lastError: Error | null = null;
    let retryCount = 0;
    let delay = this.retryOptions.retryDelay;

    while (retryCount <= this.retryOptions.maxRetries) {
      try {
        if (retryCount > 0) {
          console.log(`Retrying handler at index ${handlerIndex}, attempt ${retryCount}/${this.retryOptions.maxRetries}`);
        }
        return await handler(message, this.initialState, state);
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        retryCount++;

        if (retryCount <= this.retryOptions.maxRetries) {
          await new Promise(resolve => setTimeout(resolve, delay));
          delay *= (this.retryOptions.backoffFactor || 1);
        } else {
          break; // Exit when max retries exceeded
        }
      }
    }

    throw lastError || new Error("Maximum retries exceeded");
  }

  /**
   * Execute the workflow with multiple messages
   * @param messages Array of messages to process
   * @param options Execution options
   * @param currentState Optional current state (defaults to initialState)
   * @returns Array of results from the workflow execution
   */
  async executeBulk(
    messages: T[],
    options: WorkflowExecutionOptions,
    currentState?: WorkflowState
  ): Promise<WorkflowResult<R>[]> {
    const state = currentState ? { ...this.initialState, ...currentState } : { ...this.initialState };

    switch (options.strategy) {
      case 'queue':
        return this.executeQueue(messages, state, options.errorHandling);
      case 'parallel':
        return this.executeParallel(messages, state, options.errorHandling);
      case 'bottleneck':
        return this.executeBottleneck(messages, state, options.concurrency || 5, options.errorHandling);
      default:
        throw new Error(`Unknown execution strategy: ${options.strategy}`);
    }
  }

  /**
   * Execute messages in a sequential queue
   * @private
   */
  private async executeQueue(
    messages: T[],
    state: WorkflowState,
    errorHandling?: ErrorHandlingStrategy
  ): Promise<WorkflowResult<R>[]> {
    const results: WorkflowResult<R>[] = [];

    for (const message of messages) {
      const result = await this.execute(message, { ...state }, { errorHandling });
      results.push(result);

      // If fail-fast and we had an error, stop processing
      if (errorHandling === 'fail-fast' && !result.success) {
        break;
      }
    }

    return results;
  }

  /**
   * Execute all messages in parallel
   * @private
   */
  private async executeParallel(
    messages: T[],
    state: WorkflowState,
    errorHandling?: ErrorHandlingStrategy
  ): Promise<WorkflowResult<R>[]> {
    const promises = messages.map(message => this.execute(message, state, { errorHandling }));
    return Promise.all(promises);
  }

  /**
   * Execute messages in parallel with a concurrency limit
   * @private
   */
  private async executeBottleneck(
    messages: T[],
    state: WorkflowState,
    concurrency: number,
    errorHandling?: ErrorHandlingStrategy
  ): Promise<WorkflowResult<R>[]> {
    const results: WorkflowResult<R>[] = [];
    const messageQueue = [...messages];
    let hasFailedFast = false;

    // Process messages in batches with limited concurrency
    while (messageQueue.length > 0 && !hasFailedFast) {
      const batchSize = Math.min(concurrency, messageQueue.length);
      const batch = messageQueue.splice(0, batchSize);

      const batchResults = await Promise.all(
        batch.map(async (message) => {
          const result = await this.execute(message, state, { errorHandling });

          // If using fail-fast strategy and we got an error, flag to stop processing
          if (errorHandling === 'fail-fast' && !result.success) {
            hasFailedFast = true;
          }

          return result;
        })
      );

      results.push(...batchResults);

      // Stop processing if we had a failure with fail-fast strategy
      if (hasFailedFast) {
        break;
      }
    }

    return results;
  }
}

/**
 * Create a new workflow instance
 * @param initialState Optional initial state for the workflow
 * @returns A new Workflow instance
 */
export function createWorkflow<T = any, R = any>(initialState: WorkflowState = {}): Workflow<T, R> {
  return new Workflow<T, R>(initialState);
}
