import { describe, test, expect } from "bun:test";
import { createWorkflow } from '../dist/index';
import type { WorkflowState } from '../dist/index';

// Define interfaces for product data
interface Product {
  id: string;
  name: string;
  netPrice: number;
  category: string;
}

interface ProductWithPrice extends Product {
  grossPrice: number;
  discount?: number;
  finalPrice?: number;
}

interface PricingResult {
  id: number;
  name: string;
  price: number;
  tax: number;
  totalPrice: number;
}

// Define handlers for price calculations
const applyVatHandler = async (product: Product, initialState: WorkflowState, currentState: WorkflowState) => {
  const vatRates = currentState.vatRates || { standard: 0.23, reduced: 0.08, exempt: 0 };
  const rate = product.category === 'food' ? vatRates.reduced :
    product.category === 'books' ? vatRates.reduced :
      product.category === 'medical' ? vatRates.exempt :
        vatRates.standard;

  return {
    ...product,
    grossPrice: +(product.netPrice * (1 + rate)).toFixed(2)
  };
};

const applyDiscountHandler = async (product: ProductWithPrice, initialState: WorkflowState, currentState: WorkflowState) => {
  const discounts = currentState.discounts || {};
  const discountPercent = discounts[product.category] || 0;

  return {
    ...product,
    discount: discountPercent,
    finalPrice: +(product.grossPrice * (1 - discountPercent)).toFixed(2)
  };
};

const calculatePriceWithTax = async (product: any): Promise<PricingResult> => {
  const taxRate = 0.1; // 10% tax
  const tax = product.price * taxRate;
  return {
    id: product.id,
    name: product.name,
    price: product.price,
    tax: tax,
    totalPrice: product.price + tax
  };
};

describe('Price Calculation Workflow', () => {
  test('single product price calculation', async () => {
    const product: Product = {
      id: "prod-001",
      name: "Laptop",
      netPrice: 1000,
      category: "electronics"
    };

    const priceWorkflow = createWorkflow({
      vatRates: { standard: 0.23, reduced: 0.08, exempt: 0 }
    });

    priceWorkflow
      .addHandler(applyVatHandler)
      .addHandler(applyDiscountHandler);

    const response = await priceWorkflow.execute(product);
    expect(response.success).toBe(true);
    const result = response.result as ProductWithPrice;

    expect(result.grossPrice).toBe(1230); // 1000 + 23% VAT
    expect(result.finalPrice).toBe(1230); // No discount applied
  });

  test('bulk price calculation - queue strategy', async () => {
    const products: Product[] = [
      { id: "prod-001", name: "Laptop", netPrice: 1000, category: "electronics" },
      { id: "prod-002", name: "Book", netPrice: 50, category: "books" },
      { id: "prod-003", name: "Apple", netPrice: 2, category: "food" },
      { id: "prod-004", name: "Medicine", netPrice: 100, category: "medical" },
    ];

    const priceWorkflow = createWorkflow({
      vatRates: { standard: 0.23, reduced: 0.08, exempt: 0 },
      discounts: { books: 0.10, electronics: 0.05 }
    });

    priceWorkflow
      .addHandler(applyVatHandler)
      .addHandler(applyDiscountHandler);

    const responses = await priceWorkflow.execute(products, { strategy: 'queue' });
    expect(responses.every(r => r.success)).toBe(true);

    const results = responses.map(r => r.result as ProductWithPrice);

    // Check results
    expect(results[0]?.grossPrice).toBe(1230); // 1000 + 23% VAT
    expect(results[0]?.finalPrice).toBe(1168.50); // 5% discount on electronics

    expect(results[1]?.grossPrice).toBe(54); // 50 + 8% VAT
    expect(results[1]?.finalPrice).toBe(48.60); // 10% discount on books

    expect(results[2]?.grossPrice).toBe(2.16); // 2 + 8% VAT
    expect(results[2]?.finalPrice).toBe(2.16); // No discount on food

    expect(results[3]?.grossPrice).toBe(100); // No VAT on medical
    expect(results[3]?.finalPrice).toBe(100); // No discount on medical
  });

  test('bulk price calculation - parallel strategy', async () => {
    const products: Product[] = [
      { id: "prod-001", name: "Laptop", netPrice: 1000, category: "electronics" },
      { id: "prod-002", name: "Book", netPrice: 50, category: "books" },
      { id: "prod-003", name: "Apple", netPrice: 2, category: "food" },
      { id: "prod-004", name: "Medicine", netPrice: 100, category: "medical" },
    ];

    const priceWorkflow = createWorkflow({
      vatRates: { standard: 0.23, reduced: 0.08, exempt: 0 },
      discounts: { books: 0.10, electronics: 0.05 }
    });

    priceWorkflow
      .addHandler(applyVatHandler)
      .addHandler(applyDiscountHandler);

    const responses = await priceWorkflow.execute(products, { strategy: 'parallel' });
    expect(responses.every(r => r.success)).toBe(true);

    const results = responses.map(r => r.result as ProductWithPrice);

    // Check results (same expectations as queue strategy)
    expect(results[0]?.grossPrice).toBe(1230);
    expect(results[0]?.finalPrice).toBe(1168.50);
    expect(results[1]?.grossPrice).toBe(54);
    expect(results[1]?.finalPrice).toBe(48.60);
    expect(results[2]?.grossPrice).toBe(2.16);
    expect(results[2]?.finalPrice).toBe(2.16);
    expect(results[3]?.grossPrice).toBe(100);
    expect(results[3]?.finalPrice).toBe(100);
  });

  test('bulk price calculation - bottleneck strategy', async () => {
    // Simulating a large inventory of products
    const products: Product[] = [
      { id: "prod-001", name: "Laptop", netPrice: 1000, category: "electronics" },
      { id: "prod-002", name: "Phone", netPrice: 800, category: "electronics" },
      { id: "prod-003", name: "Tablet", netPrice: 600, category: "electronics" },
      { id: "prod-004", name: "Book", netPrice: 50, category: "books" },
      { id: "prod-005", name: "Medicine", netPrice: 100, category: "medical" },
    ];

    const priceWorkflow = createWorkflow({
      vatRates: { standard: 0.23, reduced: 0.08, exempt: 0 },
      discounts: { electronics: 0.05 }
    });

    priceWorkflow
      .addHandler(applyVatHandler)
      .addHandler(applyDiscountHandler);

    const responses = await priceWorkflow.execute(
      products,
      { strategy: 'bottleneck', concurrency: 2 }
    );
    expect(responses.every(r => r.success)).toBe(true);

    const results = responses.map(r => r.result as ProductWithPrice);

    // Check results for electronic items with discount
    expect(results[0]?.grossPrice).toBe(1230);
    expect(results[0]?.finalPrice).toBe(1168.50);
    expect(results[1]?.grossPrice).toBe(984);
    expect(results[1]?.finalPrice).toBe(934.80);
    expect(results[2]?.grossPrice).toBe(738);
    expect(results[2]?.finalPrice).toBe(701.10);
  });

  test('bulk price calculation - bottleneck strategy with rate limiting', async () => {
    const workflow = createWorkflow<Product, PricingResult>();
    workflow.addHandler(calculatePriceWithTax);

    const products = [
      { id: 1, name: 'Product 1', price: 100 },
      { id: 2, name: 'Product 2', price: 200 },
      { id: 3, name: 'Product 3', price: 300 },
      { id: 4, name: 'Product 4', price: 400 },
      { id: 5, name: 'Product 5', price: 500 },
    ];

    const startTime = Date.now();
    const results = await workflow.execute(
      products,
      { strategy: 'bottleneck', concurrency: 2, rateLimit: 5 } // 5 requests per second
    );
    const endTime = Date.now();

    // With 5 products, concurrency of 2, and rate limit of 5 req/sec,
    // this should take at least 200ms due to rate limiting
    const executionTime = endTime - startTime;
    expect(executionTime).toBeGreaterThanOrEqual(200);

    expect(results.length).toBe(5);
    expect(results.every(r => r.success)).toBe(true);

    const totalPrice = results.reduce((sum, r) => sum + (r.result?.totalPrice || 0), 0);
    expect(totalPrice).toBe(1650); // 1500 + 10% tax
  });

  test('bulk price calculation - bottleneck strategy with rate limit per minute', async () => {
    const workflow = createWorkflow<Product, PricingResult>();
    workflow.addHandler(calculatePriceWithTax);

    const products = [
      { id: 1, name: 'Product 1', price: 100 },
      { id: 2, name: 'Product 2', price: 200 },
      { id: 3, name: 'Product 3', price: 300 },
      { id: 4, name: 'Product 4', price: 400 },
      { id: 5, name: 'Product 5', price: 500 },
      { id: 6, name: 'Product 6', price: 600 }
    ];

    const startTime = Date.now();
    const results = await workflow.execute(
      products,
      {
        strategy: 'bottleneck',
        concurrency: 3,
        rateLimit: { value: 9000, unit: 'minute' } // 9000 requests per minute
      }
    );
    const endTime = Date.now();

    // With 6 products and rate limit of 9000/minute (6.7ms between batches),
    // this should take minimal time but still respect the rate limit
    const executionTime = endTime - startTime;

    expect(results.length).toBe(6);
    expect(results.every(r => r.success)).toBe(true);

    const totalPrice = results.reduce((sum, r) => sum + (r.result?.totalPrice || 0), 0);
    expect(totalPrice).toBe(2310); // 2100 + 10% tax
  });

  test('workflow with promo campaign state', async () => {
    // Creating a special promotion workflow with date-based discount
    const product: Product = {
      id: "prod-001",
      name: "Laptop",
      netPrice: 1000,
      category: "electronics"
    };

    const promoWorkflow = createWorkflow({
      vatRates: { standard: 0.23, reduced: 0.08, exempt: 0 },
      discounts: { electronics: 0.05 }, // 5% regular category discount
      promoActive: true,
      promoDiscount: 0.15 // 15% special discount
    });

    // Custom promotion handler that applies special discount if promo is active
    const applyPromoHandler = async (product: ProductWithPrice, initialState: WorkflowState, currentState: WorkflowState) => {
      if (currentState.promoActive && product.finalPrice) {
        return {
          ...product,
          promoApplied: true,
          finalPrice: +(product.finalPrice * (1 - currentState.promoDiscount)).toFixed(2)
        };
      }
      return product;
    };

    promoWorkflow
      .addHandler(applyVatHandler)
      .addHandler(applyDiscountHandler)
      .addHandler(applyPromoHandler);

    const response = await promoWorkflow.execute(product);
    expect(response.success).toBe(true);

    const result = response.result as ProductWithPrice & { promoApplied: boolean };

    // Manual calculation to verify:
    // Net price: 1000
    // With VAT (23%): 1000 * 1.23 = 1230
    // With category discount (5%): 1230 * 0.95 = 1168.50
    // With promo discount (15%): 1168.50 * 0.85 = 993.23
    expect(result.grossPrice).toBe(1230); // 1000 + 23% VAT
    expect(result.finalPrice).toBe(993.23); // With both discounts applied
    expect(result.promoApplied).toBe(true);
  });
});