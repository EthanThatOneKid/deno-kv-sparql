import { assertEquals } from "@std/assert";
import { kvSparql } from "./sparql.ts";

Deno.test({
  permissions: { sys: true },
  name: "kvSparql executes valid SPARQL queries",
  fn: async (t) => {
    await using kv = await Deno.openKv(":memory:");

    await t.step("insert data", async () => {
      const result = await kvSparql(
        kv,
        ["test"],
        "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }",
      );
      assertEquals(result.resultType, "void");
    });

    await t.step("query data", async () => {
      const result = await kvSparql(
        kv,
        ["test"],
        "SELECT * WHERE { ?s ?p ?o }",
      );

      if (result.resultType !== "bindings") {
        throw new Error(`Expected bindings, got ${result.resultType}`);
      }

      const bindings = await result.data.toArray();
      const entry = bindings[0];
      assertEquals(entry.get("s").value, "http://example.org/s");
      assertEquals(entry.get("p").value, "http://example.org/p");
      assertEquals(entry.get("o").value, "http://example.org/o");
    });
  },
});
