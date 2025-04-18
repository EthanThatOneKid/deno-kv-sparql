# deno-kv-sparql

Execute SPARQL queries on Deno Kv storage.

## Example

```ts
await kvSparql(
  kv,
  ["example"],
  "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }",
);

const result = await kvSparql(
  kv,
  ["example"],
  "SELECT * WHERE { ?s ?p ?o }",
);
```

## Develop

Run unit tests.

```sh
deno task test
```

Format the code.

```sh
deno fmt
```

Update outdated dependencies to their latest versions.

```sh
deno task outdated
```

---

Developed with <3 [**@FartLabs**](https://github.com/FartLabs)
