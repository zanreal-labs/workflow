// Types and interfaces
export type WorkflowState = Record<string, any>;
export type WorkflowHandler<T = any, R = any> = (message: T, initialState: WorkflowState, currentState: WorkflowState) => Promise<R>;

export interface WorkflowExecutionOptions {
  strategy: 'queue' | 'parallel' | 'bottleneck';
  concurrency?: number; // For bottleneck strategy
}

/**
 * Workflow class for processing messages through a series of handlers
 */
export class Workflow<T = any, R = any> {
  private handlers: WorkflowHandler<T, R>[] = [];
  private initialState: WorkflowState = {};

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
   * Execute the workflow with a single message
   * @param message The message to process
   * @param currentState Optional current state (defaults to initialState)
   * @returns Result of the workflow execution
   */
  async execute(message: T, currentState?: WorkflowState): Promise<R | undefined> {
    const state = currentState ? { ...this.initialState, ...currentState } : { ...this.initialState };
    let result: any = message;
    
    for (const handler of this.handlers) {
      result = await handler(result, this.initialState, state);
    }
    
    return result as R;
  }

  /**
   * Execute the workflow with multiple messages
   * @param messages Array of messages to process
   * @param options Execution options
   * @param currentState Optional current state (defaults to initialState)
   * @returns Array of results from the workflow execution
   */
  async executeBulk(messages: T[], options: WorkflowExecutionOptions, currentState?: WorkflowState): Promise<(R | undefined)[]> {
    const state = currentState ? { ...this.initialState, ...currentState } : { ...this.initialState };
    
    switch (options.strategy) {
      case 'queue':
        return this.executeQueue(messages, state);
      case 'parallel':
        return this.executeParallel(messages, state);
      case 'bottleneck':
        return this.executeBottleneck(messages, state, options.concurrency || 5);
      default:
        throw new Error(`Unknown execution strategy: ${options.strategy}`);
    }
  }

  /**
   * Execute messages in a sequential queue
   * @private
   */
  private async executeQueue(messages: T[], state: WorkflowState): Promise<(R | undefined)[]> {
    const results: (R | undefined)[] = [];

    for (const message of messages) {
      results.push(await this.execute(message, state));
    }

    return results;
  }

  /**
   * Execute all messages in parallel
   * @private
   */
  private async executeParallel(messages: T[], state: WorkflowState): Promise<(R | undefined)[]> {
    const promises = messages.map(message => this.execute(message, state));
    return Promise.all(promises);
  }

  /**
   * Execute messages in parallel with a concurrency limit
   * @private
   */
  private async executeBottleneck(messages: T[], state: WorkflowState, concurrency: number): Promise<(R | undefined)[]> {
    const results: (R | undefined)[] = [];
    const pendingPromises: Promise<void>[] = [];
    const messageQueue = [...messages];

    const runTask = async (): Promise<void> => {
      if (messageQueue.length === 0) return;

      const message = messageQueue.shift()!;
      const result = await this.execute(message, state);
      results.push(result);

      if (messageQueue.length > 0) {
        await runTask();
      }
    };

    // Start initial batch of tasks
    const initialBatchSize = Math.min(concurrency, messageQueue.length);
    for (let i = 0; i < initialBatchSize; i++) {
      pendingPromises.push(runTask());
    }
    
    await Promise.all(pendingPromises);
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

export default function main() {
  console.log("Hello, world!");
}