// deno-types="@types/n3"
import { BindingsStream, Parser, Quad, ResultStream, Store, Writer } from "n3";
import { QueryEngine } from "@comunica/query-sparql-rdfjs-lite";
import { getAsBlob, set as setBlob } from "@kitsonk/kv-toolbox/blob";

/**
 * kvSparql executes a SPARQL query against an n3 store in Deno.Kv storage.
 */
export async function kvSparql(
  kv: Deno.Kv,
  key: Deno.KvKey,
  query: string,
  options?: KvSparqlOptions,
): Promise<SparqlResult> {
  const store = (await getStore(kv, key, options?.consistency)) ??
    new Store<Quad, Quad, Quad, Quad>();
  const result = await sparql(store, query);
  await setStore(kv, key, store, {
    expireIn: options?.expireIn,
    format: options?.format,
  });

  return result;
}

/**
 * sparql executes a SPARQL query against an n3 store.
 */
export async function sparql(
  store: Store,
  query: string,
  engine = new QueryEngine(),
): Promise<SparqlResult> {
  const parsedQuery = await engine.query(query, { sources: [store] });
  return {
    resultType: parsedQuery.resultType,
    data: await parsedQuery.execute(),
  } as SparqlResult;
}

/**
 * SparqlResult is the result of a SPARQL query.
 */
export type SparqlResult =
  | { resultType: "bindings"; data: BindingsStream }
  | { resultType: "boolean"; data: boolean }
  | { resultType: "quads"; data: AsyncIterator<Quad> & ResultStream<Quad> }
  | { resultType: "void"; data: void };

/**
 * SparqlResultType is the type of a SPARQL query result.
 */
export type SparqlResultType = (typeof sparqlResultTypes)[number];

/**
 * sparqlResultTypes are the possible result types of a SPARQL query.
 */
export const sparqlResultTypes = [
  "bindings",
  "boolean",
  "quads",
  "void",
] as const;

/**
 * setStore sets an n3 store in Deno.Kv storage.
 */
export async function setStore(
  kv: Deno.Kv,
  key: Deno.KvKey,
  store: Store,
  options?: Pick<KvSparqlOptions, "expireIn" | "format">,
): Promise<Deno.KvCommitResult> {
  // https://github.com/rdfjs/N3.js#writing
  const writer = new Writer({ format: options?.format });
  const result = writer.quadsToString(store.toArray());
  const blob = new Blob([new TextEncoder().encode(result)], {
    type: options?.format,
  });

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
  consistency?: Deno.KvConsistencyLevel,
): Promise<Store<Quad, Quad, Quad, Quad> | null> {
  const store = new Store<Quad, Quad, Quad, Quad>();
  const file = await getAsBlob(kv, key, { consistency });
  if (file !== null) {
    // https://github.com/rdfjs/N3.js#parsing
    const parser = new Parser({ format: file.type || undefined });
    const quads = parser.parse(await file.text());
    store.addQuads(quads);
    return store;
  }

  return null;
}

/**
 * KvSparqlOptions are the options for kvSparql.
 */
export interface KvSparqlOptions {
  /**
   * consistency is the consistency level for the Kv store.
   */
  consistency?: Deno.KvConsistencyLevel;

  /**
   * expireIn is the expiration time for the Kv store.
   */
  expireIn?: number;

  /**
   * format is the serlization format of the store.
   */
  format?: string;
}
