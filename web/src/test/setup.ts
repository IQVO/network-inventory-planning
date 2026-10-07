import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach, vi } from "vitest";

// Explicit cleanup keeps Testing Library's unmounting independent of the
// `globals` flag; unstubAllGlobals puts the real fetch back after every test
// that installed a mock (see fetchMock.ts).
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});
