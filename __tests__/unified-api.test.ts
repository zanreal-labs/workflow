import { describe, test, expect } from "bun:test";
import { createWorkflow } from '../dist/index';
import type { WorkflowState } from '../dist/index';

// Define test interfaces
interface Product {
  id: number;
  name: string;
  price: number;
}

interface PricedProduct {
  id: number;
  name: string;
  price: number;
  taxAmount: number;
  totalPrice: number;
}

// Test handlers
const calculateTax = async (product: Product): Promise<PricedProduct> => {
  const taxRate = 0.1; // 10% tax
  const taxAmount = product.price * taxRate;
  return {
    ...product,
    taxAmount,
    totalPrice: product.price + taxAmount
  };
};

describe('Unified Workflow API', () => {
  test('auto-detection of single item vs array', async () => {
    // Create workflow with global options
    const workflow = createWorkflow<Product, PricedProduct>({
      strategy: 'bottleneck',
      concurrency: 3,
      rateLimit: { value: 10, unit: 'second' }
    });
    workflow.addHandler(calculateTax);

    // Single item execution
    const singleProduct = { id: 1, name: 'Product 1', price: 100 };
    const singleResult = await workflow.execute(singleProduct);

    expect(singleResult).toMatchObject({
      success: true,
      result: {
        id: 1,
        name: 'Product 1',
        price: 100,
        taxAmount: 10,
        totalPrice: 110
      }
    });

    // Array execution with auto-detection (uses bottleneck strategy from global options)
    const products = [
      { id: 1, name: 'Product 1', price: 100 },
      { id: 2, name: 'Product 2', price: 200 },
      { id: 3, name: 'Product 3', price: 300 }
    ];

    const arrayResults = await workflow.execute(products);

    expect(Array.isArray(arrayResults)).toBe(true);
    expect((arrayResults as any[]).length).toBe(3);
    expect((arrayResults as any[])[1].result.totalPrice).toBe(220); // 200 + 10% tax
  });

  test('overriding execution options', async () => {
    // Create workflow with global options for queue
    const workflow = createWorkflow<Product, PricedProduct>({
      strategy: 'queue',
    });
    workflow.addHandler(calculateTax);

    const products = [
      { id: 1, name: 'Product 1', price: 100 },
      { id: 2, name: 'Product 2', price: 200 },
      { id: 3, name: 'Product 3', price: 300 }
    ];

    // Override to use parallel strategy for this execution
    const results = await workflow.execute(products, { strategy: 'parallel' });

    expect(Array.isArray(results)).toBe(true);
    expect((results as any[]).length).toBe(3);
    expect((results as any[]).every(r => r.success)).toBe(true);
  });

  test('with state and retry options', async () => {
    const workflow = createWorkflow<Product, PricedProduct>({
      // Global state
      taxRates: {
        standard: 0.1,
        reduced: 0.05
      },
      // Global options
      errorHandling: 'retry',
      retryOptions: {
        maxRetries: 2,
        retryDelay: 100,
        backoffFactor: 1.5
      }
    });

    // Custom handler that uses state
    const calculateTaxWithState = async (product: Product, initialState: WorkflowState, currentState: WorkflowState): Promise<PricedProduct> => {
      const taxRate = currentState.taxRates?.standard || 0.1;
      const taxAmount = product.price * taxRate;
      return {
        ...product,
        taxAmount,
        totalPrice: product.price + taxAmount
      };
    };

    workflow.addHandler(calculateTaxWithState);

    // Execute with additional state
    const product = { id: 1, name: 'Product 1', price: 100 };
    const result = await workflow.execute(product, {
      state: {
        taxRates: {
          standard: 0.2  // Override tax rate for this execution
        }
      }
    });

    expect((result as any).result.taxAmount).toBe(20); // 20% tax instead of 10%
    expect((result as any).result.totalPrice).toBe(120);
  });
});