type BunTestExpectation = {
  toBe(value: unknown): void;
  toBeUndefined(): void;
  toMatch(pattern: RegExp | string): void;
  toEqual(value: unknown): void;
  toThrow(): void;
  toContain(value: string): void;
  toHaveLength(value: number): void;
  toBeDefined(): void;
  toBeNull(): void;
  toBeTruthy(): void;
  toBeFalsy(): void;
  not: {
    toBe(value: unknown): void;
    toContain(value: string): void;
    toEqual(value: unknown): void;
  };
};

declare global {
  function test(name: string, fn: () => unknown | Promise<unknown>): void;
  function expect(value: unknown): BunTestExpectation;
}

export {};
