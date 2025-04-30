// Types and interfaces
export type WorkflowState = Record<string, any>;
export type WorkflowHandler<T = any, R = any> = (message: T, initialState: WorkflowState, currentState: WorkflowState) => Promise<R>;

export type ExecutionStrategy = 'queue' | 'parallel' | 'bottleneck';
export type ErrorHandlingStrategy = 'fail-fast' | 'continue' | 'retry';
export type RateLimitUnit = 'second' | 'minute' | 'hour' | 'day';

export interface RateLimit {
  value: number;
  unit: RateLimitUnit;
}

export interface WorkflowOptions {
  strategy?: ExecutionStrategy;
  concurrency?: number;
  rateLimit?: number | RateLimit;
  errorHandling?: ErrorHandlingStrategy;
  retryOptions?: RetryOptions;
}

export interface ExecuteOptions {
  strategy?: ExecutionStrategy;
  concurrency?: number;
  rateLimit?: number | RateLimit;
  errorHandling?: ErrorHandlingStrategy;
  state?: WorkflowState;
  retryOptions?: RetryOptions;
}

export interface RetryOptions {
  maxRetries: number;
  retryDelay: number;
  backoffFactor?: number;
  handlerOverrides?: Record<number, {
    maxRetries?: number;
    retryDelay?: number;
    backoffFactor?: number;
  }>;
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
  private globalOptions: WorkflowOptions = {
    strategy: 'queue',
    concurrency: 5,
    errorHandling: 'fail-fast',
    retryOptions: {
      maxRetries: 3,
      retryDelay: 500,
      backoffFactor: 2
    }
  };

  /**
   * Create a new Workflow instance
   * @param initialStateOrOptions Initial state object or workflow options
   * @param options Workflow execution options (if first param is state)
   */
  constructor(initialStateOrOptions: WorkflowState | WorkflowOptions = {}, options?: WorkflowOptions) {
    if (options) {
      // First param is state, second is options
      this.initialState = { ...initialStateOrOptions as WorkflowState };
      this.globalOptions = { ...this.globalOptions, ...options };
    } else if ('strategy' in initialStateOrOptions || 'errorHandling' in initialStateOrOptions ||
      'concurrency' in initialStateOrOptions || 'rateLimit' in initialStateOrOptions ||
      'retryOptions' in initialStateOrOptions) {
      // First param is options
      this.globalOptions = { ...this.globalOptions, ...initialStateOrOptions as WorkflowOptions };
    } else {
      // First param is state
      this.initialState = { ...initialStateOrOptions as WorkflowState };
    }
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
   * Configure workflow options
   * @param options Workflow options configuration
   * @returns The workflow instance for chaining
   */
  configure(options: Partial<WorkflowOptions>): Workflow<T, R> {
    this.globalOptions = { ...this.globalOptions, ...options };
    if (options.retryOptions) {
      this.globalOptions.retryOptions = {
        ...this.globalOptions.retryOptions,
        ...options.retryOptions
      };
    }
    return this;
  }

  /**
   * Execute the workflow with single message or multiple messages
   * @param input Single message or array of messages to process
   * @param optionsOrState Optional execution options or state object
   * @param state Optional state override (if second param is options)
   * @returns Result(s) of the workflow execution
   */
  async execute(
    input: T | T[],
    optionsOrState?: ExecuteOptions | WorkflowState,
    state?: WorkflowState
  ): Promise<WorkflowResult<R> | WorkflowResult<R>[]> {
    // Determine if we're processing a single item or an array
    const isArray = Array.isArray(input);

    // Parse options and state
    let options: ExecuteOptions = { ...this.globalOptions };
    let currentState: WorkflowState = { ...this.initialState };

    if (optionsOrState) {
      if ('strategy' in optionsOrState || 'errorHandling' in optionsOrState ||
        'concurrency' in optionsOrState || 'rateLimit' in optionsOrState ||
        'retryOptions' in optionsOrState || 'state' in optionsOrState) {
        // Second parameter is options
        options = { ...options, ...optionsOrState as ExecuteOptions };
        if ((optionsOrState as ExecuteOptions).state) {
          currentState = { ...currentState, ...(optionsOrState as ExecuteOptions).state };
        }
        // Third parameter is additional state override
        if (state) {
          currentState = { ...currentState, ...state };
        }
      } else {
        // Second parameter is state
        currentState = { ...currentState, ...optionsOrState as WorkflowState };
      }
    }

    // If single item, process it directly
    if (!isArray) {
      return this.executeSingle(input as T, currentState, options);
    }

    // Otherwise, process as an array using the specified strategy
    return this.executeBulk(input as T[], currentState, options);
  }

  /**
   * Execute a single message through the workflow
   * @private
   */
  private async executeSingle(
    message: T,
    state: WorkflowState,
    options: ExecuteOptions
  ): Promise<WorkflowResult<R>> {
    let result: any = message;
    const errorHandling = options.errorHandling || this.globalOptions.errorHandling || 'fail-fast';
    let lastError: WorkflowError<T> | undefined = undefined;

    try {
      for (let i = 0; i < this.handlers.length; i++) {
        const handler = this.handlers[i]!;
        try {
          result = await this.executeHandlerWithRetry(handler, result, i, state, options);
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
    options: ExecuteOptions
  ): Promise<any> {
    const errorHandling = options.errorHandling || this.globalOptions.errorHandling || 'fail-fast';
    if (errorHandling !== 'retry') {
      return await handler(message, this.initialState, state);
    }

    // Get retry options with handler-specific overrides if available
    const globalRetryOptions = options.retryOptions || this.globalOptions.retryOptions || { maxRetries: 3, retryDelay: 500, backoffFactor: 2 };
    // Ensure handlerOverrides object exists before accessing it
    const handlerOverrides = globalRetryOptions.handlerOverrides && globalRetryOptions.handlerOverrides[handlerIndex];

    const maxRetries = handlerOverrides?.maxRetries ?? globalRetryOptions.maxRetries;
    let delay = handlerOverrides?.retryDelay ?? globalRetryOptions.retryDelay;
    const backoffFactor = handlerOverrides?.backoffFactor ?? globalRetryOptions.backoffFactor ?? 1;

    let lastError: Error | null = null;
    let retryCount = 0;

    // Preserve state between retry attempts
    let currentState = { ...state };

    while (retryCount <= maxRetries) {
      try {
        if (retryCount > 0) {
          console.log(`Retrying handler at index ${handlerIndex}, attempt ${retryCount}/${maxRetries}`);
        }
        return await handler(message, this.initialState, currentState);
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        retryCount++;

        // Update state with retry information that handlers might use
        currentState = {
          ...currentState,
          _retryInfo: {
            attempt: retryCount,
            maxRetries: maxRetries,
            handlerIndex: handlerIndex
          }
        };

        if (retryCount <= maxRetries) {
          await new Promise(resolve => setTimeout(resolve, delay));
          delay *= backoffFactor;
        } else {
          break; // Exit when max retries exceeded
        }
      }
    }

    throw lastError || new Error("Maximum retries exceeded");
  }

  /**
   * Execute the workflow with multiple messages
   * @private
   */
  private async executeBulk(
    messages: T[],
    state: WorkflowState,
    options: ExecuteOptions
  ): Promise<WorkflowResult<R>[]> {
    const strategy = options.strategy || this.globalOptions.strategy || 'queue';

    switch (strategy) {
      case 'queue':
        return this.executeQueue(messages, state, options.errorHandling);
      case 'parallel':
        return this.executeParallel(messages, state, options.errorHandling);
      case 'bottleneck':
        return this.executeBottleneck(messages, state, options);
      default:
        throw new Error(`Unknown execution strategy: ${strategy}`);
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
      const result = await this.executeSingle(message, state, { errorHandling });
      results.push(result as WorkflowResult<R>);

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
    const promises = messages.map(message => this.executeSingle(message, state, { errorHandling }));
    return Promise.all(promises);
  }

  /**
   * Execute messages in parallel with a concurrency limit and optional rate limiting
   * @private
   */
  private async executeBottleneck(
    messages: T[],
    state: WorkflowState,
    options: ExecuteOptions
  ): Promise<WorkflowResult<R>[]> {
    const results: WorkflowResult<R>[] = new Array(messages.length);
    const messageQueue = [...messages];
    let activeCount = 0;
    let hasFailedFast = false;
    let lastRequestTime = 0;
    let index = 0;

    // Calculate delay in milliseconds between requests based on rate limit
    const getMinDelayMs = (rateLimit: number | RateLimit): number => {
      if (typeof rateLimit === 'number') {
        // Legacy mode: treat as requests per second
        return Math.floor(1000 / rateLimit);
      } else {
        // New mode with units
        switch (rateLimit.unit) {
          case 'second':
            return Math.floor(1000 / rateLimit.value);
          case 'minute':
            return Math.floor(60000 / rateLimit.value);
          case 'hour':
            return Math.floor(3600000 / rateLimit.value);
          case 'day':
            return Math.floor(86400000 / rateLimit.value);
          default:
            return Math.floor(1000 / rateLimit.value); // Default to second
        }
      }
    };

    const minDelayMs = options.rateLimit ? getMinDelayMs(options.rateLimit) : 0;
    const concurrency = options.concurrency || this.globalOptions.concurrency || 5;
    const errorHandling = options.errorHandling || this.globalOptions.errorHandling;

    // Function to process a single message
    const processMessage = async (message: T, currentIndex: number): Promise<void> => {
      try {
        const result = await this.executeSingle(message, state, options);
        results[currentIndex] = result as WorkflowResult<R>;

        // If using fail-fast strategy and we got an error, flag to stop processing
        if (errorHandling === 'fail-fast' && !result.success) {
          hasFailedFast = true;
        }
      } catch (error) {
        // This should not happen because errors are handled in this.executeSingle,
        // but add as a safeguard
        results[currentIndex] = {
          success: false,
          error: {
            message: `Unhandled error: ${error instanceof Error ? error.message : String(error)}`,
            originalError: error instanceof Error ? error : new Error(String(error)),
            handlerIndex: -1,
            inputMessage: message,
            state: { ...state }
          }
        };

        if (errorHandling === 'fail-fast') {
          hasFailedFast = true;
        }
      }
    };

    // Process messages in batches respecting concurrency and rate limits
    const processQueue = async (): Promise<void> => {
      while (messageQueue.length > 0 && !hasFailedFast) {
        // Take up to concurrency number of messages to process in this batch
        const currentBatch: Promise<void>[] = [];

        // Process up to concurrency items at once
        for (let i = 0; i < Math.min(concurrency, messageQueue.length); i++) {
          if (hasFailedFast) break;

          // Apply rate limiting if needed
          if (minDelayMs > 0 && i > 0) {
            const now = Date.now();
            const timeToWait = Math.max(0, lastRequestTime + minDelayMs - now);
            if (timeToWait > 0) {
              await new Promise(resolve => setTimeout(resolve, timeToWait));
            }
          }

          const message = messageQueue.shift();
          if (message === undefined) break;

          const currentIndex = index++;
          lastRequestTime = Date.now();

          currentBatch.push(processMessage(message, currentIndex));
        }

        // Wait for current batch to complete before starting next batch
        await Promise.all(currentBatch);
      }
    };

    // Start processing the queue
    await processQueue();

    // Filter out any undefined results (should not happen, but just in case)
    return results.filter(r => r !== undefined);
  }
}

/**
 * Create a new workflow instance
 * @param stateOrOptions Optional initial state or options for the workflow
 * @param options Optional workflow execution options (if first param is state)
 * @returns A new Workflow instance
 */
export function createWorkflow<T = any, R = any>(
  stateOrOptions: WorkflowState | WorkflowOptions = {},
  options?: WorkflowOptions
): Workflow<T, R> {
  return new Workflow<T, R>(stateOrOptions, options);
}
