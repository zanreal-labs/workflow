import { describe, test, expect } from "bun:test";
import { createWorkflow } from '../src/index';
import type { WorkflowState } from '../src/index';

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

    const result = await priceWorkflow.execute(product);
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

    const results = await priceWorkflow.executeBulk(products, { strategy: 'queue' });
    
    // Check results
    expect(results[0].grossPrice).toBe(1230); // 1000 + 23% VAT
    expect(results[0].finalPrice).toBe(1168.50); // 5% discount on electronics
    
    expect(results[1].grossPrice).toBe(54); // 50 + 8% VAT
    expect(results[1].finalPrice).toBe(48.60); // 10% discount on books
    
    expect(results[2].grossPrice).toBe(2.16); // 2 + 8% VAT
    expect(results[2].finalPrice).toBe(2.16); // No discount on food
    
    expect(results[3].grossPrice).toBe(100); // No VAT on medical
    expect(results[3].finalPrice).toBe(100); // No discount on medical
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

    const results = await priceWorkflow.executeBulk(products, { strategy: 'parallel' });
    
    // Check results (same expectations as queue strategy)
    expect(results[0].grossPrice).toBe(1230);
    expect(results[0].finalPrice).toBe(1168.50);
    expect(results[1].grossPrice).toBe(54);
    expect(results[1].finalPrice).toBe(48.60);
    expect(results[2].grossPrice).toBe(2.16);
    expect(results[2].finalPrice).toBe(2.16);
    expect(results[3].grossPrice).toBe(100);
    expect(results[3].finalPrice).toBe(100);
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

    const results = await priceWorkflow.executeBulk(
      products,
      { strategy: 'bottleneck', concurrency: 2 }
    );
    
    // Check results for electronic items with discount
    expect(results[0].grossPrice).toBe(1230);
    expect(results[0].finalPrice).toBe(1168.50);
    expect(results[1].grossPrice).toBe(984);
    expect(results[1].finalPrice).toBe(934.80);
    expect(results[2].grossPrice).toBe(738);
    expect(results[2].finalPrice).toBe(701.10);
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
    
    const result = await promoWorkflow.execute(product);
    
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