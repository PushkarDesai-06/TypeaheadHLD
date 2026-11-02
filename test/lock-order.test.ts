import { expect, test } from "bun:test";
import { inLockOrder } from "../src/db";

test("inLockOrder sorts by UTF-8 bytes, matching Postgres COLLATE \"C\"", () => {
  const keys = ["😀", "b", "￿", "é", "Z", "a", "z"];
  expect(inLockOrder(keys)).toEqual(["Z", "a", "b", "z", "é", "￿", "😀"]);
});

test("inLockOrder differs from JS default sort for characters outside the BMP", () => {
  expect([..."￿😀"].sort()).toEqual(["😀", "￿"]);
  expect(inLockOrder(["😀", "￿"])).toEqual(["￿", "😀"]);
});
