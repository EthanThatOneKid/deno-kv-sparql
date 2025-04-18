// deno-types="@types/n3"
import { Quad, Store, StreamParser, Writer } from "n3";
import { QueryEngine } from "@comunica/query-sparql-rdfjs-lite";
import { getAsBlob, set as setBlob } from "@kitsonk/kv-toolbox/blob";
import { Stream } from "node:stream";

if (import.meta.main) {
  // TODO: Create example loading data to an n3 store via sparql.
  // Verify by reading the n3 store and executing a SPARQL query.
}

/**
 * sparql executes a SPARQL query against an n3 store.
 */
export async function sparql(
  kv: Deno.Kv,
  key: Deno.KvKey,
  query: string,
  consistency?: Deno.KvConsistencyLevel
) {
  const store = (await getStore(kv, key, consistency)) ?? new Store();
  const engine = new QueryEngine();
  const bindings = await engine.query(query, { sources: [store] });
  await setStore(kv, key, store);
  return bindings;
}

/**
 * setStore sets an n3 store in Deno.Kv storage.
 */
export async function setStore(
  kv: Deno.Kv,
  key: Deno.KvKey,
  store: Store,
  options?: { format?: string; expireIn?: number }
): Promise<Deno.KvCommitResult> {
  const writer = new Writer({ format: options?.format });
  const result = writer.quadsToString(store.toArray());
  const encoder = new TextEncoder();
  const blob = new Blob([encoder.encode(result)], { type: options?.format });
  return await setBlob(kv, key, blob, { expireIn: options?.expireIn });
}

/**
 * getStore gets an n3 store from Deno.Kv storage.
 *
 * Returns null if the key does not exist.
 */
export async function getStore(
  kv: Deno.Kv,
  key: Deno.KvKey,
  consistency?: Deno.KvConsistencyLevel
): Promise<Store<Quad, Quad, Quad, Quad> | null> {
  const store = new Store<Quad, Quad, Quad, Quad>();
  const file = await getAsBlob(kv, key, { consistency });
  if (file !== null) {
    const stream = new Stream(file.stream().getReader());
    const parser = stream.pipe(new StreamParser());
    await store.import(parser);
    return store;
  }

  return null;
}
