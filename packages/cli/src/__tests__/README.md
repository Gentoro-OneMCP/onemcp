# CLI Unit Tests

This directory contains comprehensive unit tests for the OneMCP CLI.

## Test Structure

```
src/
├── __tests__/
│   ├── types.test.ts              # Type definitions tests
│   └── README.md                  # This file
├── config/
│   └── __tests__/
│       ├── paths.test.ts          # Path management tests
│       └── manager.test.ts        # Config manager tests
├── services/
│   └── __tests__/
│       └── process-manager.test.ts # Process management tests
├── chat/
│   └── __tests__/
│       └── chat-mode.test.ts      # Chat functionality tests
└── handbook/
    └── __tests__/
        └── manager.test.ts        # Handbook management tests
```

## Running Tests

```bash
# Run all tests
npm test

# Run tests in watch mode
npm run test:watch

# Run tests with coverage
npm run test:coverage

# Run specific test file
npm test -- paths.test.ts

# Run tests matching a pattern
npm test -- --testNamePattern="PathManager"
```

## Test Coverage

The test suite covers:

- **Path Management** (`config/paths.test.ts`)
  - Singleton pattern
  - Path generation for config, logs, PIDs, handbooks
  - Service-specific path handling

- **Configuration Management** (`config/manager.test.ts`)
  - Loading and saving global config
  - Service configuration CRUD operations
  - YAML parsing and validation
  - Error handling

- **Process Management** (`services/process-manager.test.ts`)
  - Process registration and lifecycle
  - Spawning detached processes
  - Health checks and status monitoring
  - Error handling for optional/required services
  - PID file management

- **Chat Mode** (`chat/chat-mode.test.ts`)
  - Message sending to REST API
  - Error handling (network, API errors)
  - Message history management
  - URL transformation

- **Handbook Management** (`handbook/manager.test.ts`)
  - Handbook initialization and validation
  - Directory structure creation
  - CRUD operations
  - Agent.yaml content management

- **Type Definitions** (`types.test.ts`)
  - Type safety validation
  - Interface compliance
  - Optional field handling

## Writing New Tests

### Test File Naming

- Test files should be named `*.test.ts`
- Place tests in `__tests__` directory alongside the code they test
- Example: `src/services/foo.ts` → `src/services/__tests__/foo.test.ts`

### Test Structure

```typescript
import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import { YourModule } from '../your-module.js';

// Mock external dependencies
jest.mock('external-module');

describe('YourModule', () => {
  let instance: YourModule;

  beforeEach(() => {
    jest.clearAllMocks();
    instance = new YourModule();
  });

  describe('methodName', () => {
    it('should do something specific', () => {
      // Arrange
      const input = 'test';
      
      // Act
      const result = instance.methodName(input);
      
      // Assert
      expect(result).toBe('expected');
    });

    it('should handle error cases', () => {
      expect(() => instance.methodName(null)).toThrow();
    });
  });
});
```

### Mocking Guidelines

1. **Mock external dependencies**: Always mock I/O operations (fs, network, etc.)
2. **Mock constructors carefully**: Use `jest.mock()` at module level
3. **Reset mocks**: Use `jest.clearAllMocks()` in `beforeEach`
4. **Type mocks**: Cast mocks to proper types for TypeScript

### Best Practices

1. **Descriptive test names**: Use "should..." format
2. **One assertion per test**: Keep tests focused
3. **Arrange-Act-Assert**: Follow AAA pattern
4. **Test edge cases**: Cover success, failure, and boundary conditions
5. **Avoid implementation details**: Test behavior, not internals
6. **Use `beforeEach`**: Set up clean state for each test

## Coverage Goals

- **Statements**: > 80%
- **Branches**: > 75%
- **Functions**: > 80%
- **Lines**: > 80%

## CI/CD Integration

Tests run automatically on:
- Push to any branch
- Pull request creation/update
- Pre-commit hooks (if configured)

## Troubleshooting

### ESM Issues

If you encounter ES module errors:
```bash
NODE_OPTIONS=--experimental-vm-modules npm test
```

### Mock Not Working

Ensure mocks are defined before imports:
```typescript
jest.mock('./module');  // Must be before import
import { MyClass } from './module';
```

### Type Errors in Tests

Cast to `any` when accessing private members for testing:
```typescript
const result = (instance as any).privateMethod();
```

## Additional Resources

- [Jest Documentation](https://jestjs.io/docs/getting-started)
- [Testing Best Practices](https://github.com/goldbergyoni/javascript-testing-best-practices)
- [TypeScript Testing](https://www.typescriptlang.org/docs/handbook/testing.html)

