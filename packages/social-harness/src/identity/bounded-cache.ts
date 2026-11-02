/** @file Bounded least-recently-used caches for immutable identity material. */
import { Option } from "effect";

/**
 * A map that keeps at most its capacity of entries. Reading or storing an
 * entry makes it the most recently used, and storing past the capacity drops
 * the least recently used entry, so memory stays bounded however many
 * distinct keys arrive.
 */
export interface BoundedCache<K, V> {
  /** The value for `key`, which becomes the most recently used entry. */
  readonly get: (key: K) => Option.Option<V>;
  /** Stores `value` as the most recently used entry for `key`. */
  readonly set: (key: K, value: V) => void;
}

/**
 * Creates an empty LRU cache. The caller supplies a positive integer capacity.
 */
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
