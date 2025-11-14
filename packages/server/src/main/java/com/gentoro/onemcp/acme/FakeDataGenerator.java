package com.gentoro.onemcp.acme;

import java.util.*;

/**
 * Fake Data Generator for ACME Analytics Server
 *
 * <p>Generates realistic fake data for sales, products, and customers to simulate a real e-commerce
 * analytics platform.
 */
public class FakeDataGenerator {

  private final Random random;

  public FakeDataGenerator() {
    this.random = new Random(42L); // Fixed seed for deterministic data
  }

  // Data arrays for generating realistic fake data
  private static final String[] PRODUCT_CATEGORIES = {
    "Electronics", "Clothing", "Home & Garden", "Books", "Sports & Outdoors",
    "Health & Beauty", "Toys & Games", "Automotive", "Food & Beverages", "Office Supplies"
  };

  private static final String[] ELECTRONICS_SUBCATEGORIES = {
    "Audio", "Computers", "Mobile", "Gaming", "Cameras", "Accessories"
  };

  private static final String[] CLOTHING_SUBCATEGORIES = {
    "Tops", "Bottoms", "Dresses", "Shoes", "Accessories", "Outerwear"
  };

  private static final String[] BRANDS = {
    "TechSound", "EcoWear", "HomePro", "BookMaster", "SportMax", "BeautyPlus",
    "ToyLand", "AutoParts", "FoodFresh", "OfficePro", "PremiumBrand", "BudgetChoice"
  };

  private static final String[] FIRST_NAMES = {
    "John",
    "Jane",
    "Michael",
    "Sarah",
    "David",
    "Emily",
    "Robert",
    "Jessica",
    "William",
    "Ashley",
    "James",
    "Amanda",
    "Christopher",
    "Jennifer",
    "Daniel",
    "Lisa",
    "Matthew",
    "Michelle",
    "Anthony",
    "Kimberly",
    "Mark",
    "Donna",
    "Donald",
    "Carol",
    "Steven",
    "Sandra",
    "Paul",
    "Ruth",
    "Andrew",
    "Sharon"
  };

  private static final String[] LAST_NAMES = {
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Perez",
    "Thompson",
    "White",
    "Harris",
    "Sanchez",
    "Clark",
    "Ramirez",
    "Lewis",
    "Robinson"
  };

  private static final String[] CITIES = {
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "Philadelphia",
    "San Antonio",
    "San Diego",
    "Dallas",
    "San Jose",
    "Austin",
    "Jacksonville",
    "Fort Worth",
    "Columbus",
    "Charlotte",
    "San Francisco",
    "Indianapolis",
    "Seattle",
    "Denver",
    "Washington",
    "Boston",
    "El Paso",
    "Nashville",
    "Detroit"
  };

  private static final String[] STATES = {
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI", "NJ", "VA",
    "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI", "CO", "MN", "SC", "AL"
  };

  private static final String[] PAYMENT_METHODS = {
    "credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"
  };

  private static final String[] SALE_STATUSES = {
    "completed", "pending", "cancelled", "refunded", "failed"
  };

  private static final String[] LOYALTY_TIERS = {"Bronze", "Silver", "Gold", "Platinum"};

  private static final String[] GENDERS = {"M", "F", "O", "P"};

  /** Generate fake customers */
  public List<Map<String, Object>> generateCustomers(int count) {
    List<Map<String, Object>> customers = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      Map<String, Object> customer = new HashMap<>();

      String firstName = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
      String lastName = LAST_NAMES[random.nextInt(LAST_NAMES.length)];

      customer.put("id", "CUST-" + String.format("%06d", i + 1));
      customer.put("name", firstName + " " + lastName);
      customer.put("email", firstName.toLowerCase() + "." + lastName.toLowerCase() + "@email.com");
      customer.put(
          "phone",
          "+1-"
              + String.format("%03d", random.nextInt(900) + 100)
              + "-"
              + String.format("%04d", random.nextInt(9000) + 1000));
      customer.put("age", random.nextInt(62) + 18);
      customer.put("gender", GENDERS[random.nextInt(GENDERS.length)]);
      customer.put("city", CITIES[random.nextInt(CITIES.length)]);
      customer.put("state", STATES[random.nextInt(STATES.length)]);
      customer.put("country", "USA");
      customer.put("zip_code", String.format("%05d", random.nextInt(90000) + 10000));
      customer.put("registration_date", generateRandomDate(2020, 2023));
      customer.put("loyalty_tier", LOYALTY_TIERS[random.nextInt(LOYALTY_TIERS.length)]);
      customer.put("total_spent", random.nextDouble() * 10000);
      customer.put("total_orders", random.nextInt(50));

      customers.add(customer);
    }

    return customers;
  }

  /** Generate fake products */
  public List<Map<String, Object>> generateProducts(int count) {
    List<Map<String, Object>> products = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      Map<String, Object> product = new HashMap<>();

      String category = PRODUCT_CATEGORIES[random.nextInt(PRODUCT_CATEGORIES.length)];
      String subcategory = getSubcategoryForCategory(category);
      String brand = BRANDS[random.nextInt(BRANDS.length)];

      product.put("id", "PROD-" + String.format("%06d", i + 1));
      product.put("name", generateProductName(category, subcategory, brand));
      product.put("category", category);
      product.put("subcategory", subcategory);
      product.put("brand", brand);
      product.put("price", random.nextDouble() * 990 + 10);
      product.put("cost", random.nextDouble() * 495 + 5);
      product.put("inventory", random.nextInt(200));
      product.put("rating", random.nextDouble() * 4 + 1);
      product.put("weight", random.nextDouble() * 9.9 + 0.1);
      product.put("dimensions", generateDimensions());

      products.add(product);
    }

    return products;
  }

  /** Generate fake sales */
  public List<Map<String, Object>> generateSales(
      int count, List<Map<String, Object>> customers, List<Map<String, Object>> products) {
    List<Map<String, Object>> sales = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      Map<String, Object> sale = new HashMap<>();

      // Select random customer and product
      Map<String, Object> customer = customers.get(random.nextInt(customers.size()));
      Map<String, Object> product = products.get(random.nextInt(products.size()));

      double productPrice = (Double) product.get("price");
      int quantity = random.nextInt(4) + 1;
      double discount = random.nextDouble() * (productPrice * 0.3);
      double amount = (productPrice * quantity) - discount;
      double tax = amount * 0.08; // 8% tax
      double shippingCost = amount > 50 ? 0 : random.nextDouble() * 10 + 5;

      sale.put("id", "SAL-" + String.format("%06d", i + 1));
      sale.put("customer_id", customer.get("id"));
      sale.put("product_id", product.get("id"));
      sale.put("amount", Math.round(amount * 100.0) / 100.0);
      sale.put("date", generateRandomDate(2023, 2024));
      sale.put("quantity", quantity);
      sale.put("discount", Math.round(discount * 100.0) / 100.0);
      sale.put("tax", Math.round(tax * 100.0) / 100.0);
      sale.put("shipping_cost", Math.round(shippingCost * 100.0) / 100.0);
      sale.put("payment_method", PAYMENT_METHODS[random.nextInt(PAYMENT_METHODS.length)]);
      sale.put("status", SALE_STATUSES[random.nextInt(SALE_STATUSES.length)]);

      sales.add(sale);
    }

    return sales;
  }

  /** Get subcategory for a given category */
  private String getSubcategoryForCategory(String category) {
    switch (category) {
      case "Electronics":
        return ELECTRONICS_SUBCATEGORIES[random.nextInt(ELECTRONICS_SUBCATEGORIES.length)];
      case "Clothing":
        return CLOTHING_SUBCATEGORIES[random.nextInt(CLOTHING_SUBCATEGORIES.length)];
      default:
        return "General";
    }
  }

  /** Generate product name based on category and brand */
  private String generateProductName(String category, String subcategory, String brand) {
    String[] nameTemplates = {
      "{brand} {subcategory} {category}",
      "{brand} Professional {subcategory}",
      "{brand} {subcategory} Pro",
      "Premium {brand} {subcategory}",
      "{brand} {subcategory} Deluxe"
    };

    String template = nameTemplates[random.nextInt(nameTemplates.length)];
    return template
        .replace("{brand}", brand)
        .replace("{subcategory}", subcategory)
        .replace("{category}", category);
  }

  /** Generate random dimensions */
  private String generateDimensions() {
    int length = random.nextInt(45) + 5;
    int width = random.nextInt(25) + 5;
    int height = random.nextInt(18) + 2;
    return length + "x" + width + "x" + height;
  }

  /** Generate random date between start and end year */
  private String generateRandomDate(int startYear, int endYear) {
    int year = random.nextInt(endYear - startYear + 1) + startYear;
    int month = random.nextInt(12) + 1;
    int day = random.nextInt(28) + 1; // Simplified - no leap year handling

    return String.format("%04d-%02d-%02d", year, month, day);
  }
}
