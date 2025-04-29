import { describe, test, expect } from "bun:test";
import { createWorkflow } from '../src/index';
import type { WorkflowState } from '../src/index';

// Define some sample handlers
const addOneHandler = async (num: number, initialState: WorkflowState, currentState: WorkflowState) => {
  return num + 1;
};

const multiplyByTwoHandler = async (num: number, initialState: WorkflowState, currentState: WorkflowState) => {
  return num * 2;
};

describe('Workflow', () => {
  test('single execution', async () => {
    const singleWorkflow = createWorkflow();
    singleWorkflow
      .addHandler(addOneHandler)
      .addHandler(multiplyByTwoHandler);

    const result = await singleWorkflow.execute(5);
    expect(result).toBe(12); // Should be 12: ((5 + 1) * 2)
  });

  test('bulk execution - queue strategy', async () => {
    const bulkWorkflow = createWorkflow();
    bulkWorkflow
      .addHandler(addOneHandler)
      .addHandler(multiplyByTwoHandler);

    const results = await bulkWorkflow.executeBulk([1, 2, 3, 4, 5], { strategy: 'queue' });
    expect(results).toEqual([4, 6, 8, 10, 12]);
  });

  test('bulk execution - parallel strategy', async () => {
    const bulkWorkflow = createWorkflow();
    bulkWorkflow
      .addHandler(addOneHandler)
      .addHandler(multiplyByTwoHandler);

    const results = await bulkWorkflow.executeBulk([1, 2, 3, 4, 5], { strategy: 'parallel' });
    expect(results).toEqual([4, 6, 8, 10, 12]);
  });

  test('bulk execution - bottleneck strategy', async () => {
    const bulkWorkflow = createWorkflow();
    bulkWorkflow
      .addHandler(addOneHandler)
      .addHandler(multiplyByTwoHandler);

    const results = await bulkWorkflow.executeBulk(
      [1, 2, 3, 4, 5],
      { strategy: 'bottleneck', concurrency: 2 }
    );
    expect(results).toEqual([4, 6, 8, 10, 12]);
  });

  test('workflow with state', async () => {
    const stateWorkflow = createWorkflow({ multiplier: 3 });

    const stateHandler = async (num: number, initialState: WorkflowState, currentState: WorkflowState) => {
      const multiplier = currentState.multiplier || 1;
      return num * multiplier;
    };

    stateWorkflow.addHandler(stateHandler);
    const result = await stateWorkflow.execute(5);
    expect(result).toBe(15); // Should be 15 (5 * 3)
  });
});