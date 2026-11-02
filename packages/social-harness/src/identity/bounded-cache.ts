/** @file Bounded least-recently-used caches for immutable identity material. */
import { Option } from "effect";

/** Bounds memory by evicting the least recently read or written entry at capacity. */
export interface BoundedCache<K, V> {
  readonly get: (key: K) => Option.Option<V>;

  readonly set: (key: K, value: V) => void;
}

/** Requires a positive integer capacity. */
export const makeBoundedCache = <K, V>(
  capacity: number,
): BoundedCache<K, V> => {
  const entries = new Map<K, { readonly value: V }>();
  const touch = (key: K, entry: { readonly value: V }): void => {
    entries.delete(key);
    entries.set(key, entry);
  };
  return Object.freeze({
    get: (key: K): Option.Option<V> => {
      const entry = entries.get(key);
      if (entry === undefined) {
        return Option.none();
      }
      touch(key, entry);
      return Option.some(entry.value);
    },
    set: (key: K, value: V): void => {
      touch(key, { value });
      if (entries.size > capacity) {
        const oldest = entries.keys().next();
        if (oldest.done !== true) {
          entries.delete(oldest.value);
        }
      }
    },
  });
};
