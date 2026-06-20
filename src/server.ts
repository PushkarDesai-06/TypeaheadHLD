import { SearchTrie } from "./trie";

// Initialize our in-memory data structure
const trie = new SearchTrie();

trie.insert("hello");
trie.insert("hemmoo");
trie.insert("hemmoo");
trie.insert("hemmoo");
// (You would normally load your dataset.csv into the trie here on startup)

export const server = Bun.serve({
  port: 3000,
  async fetch(req) {
    const url = new URL(req.url);

    // 1. GET /suggest?q=<prefix>
    if (req.method === "GET" && url.pathname === "/suggest") {
      const query = url.searchParams.get("q");
      console.log(query);
      if (!query) return new Response("Missing query", { status: 400 });

      const suggestions = trie.getTopSuggestions(query, 5);
      console.log(suggestions);
      return Response.json({ prefix: query, suggestions });
    }

    // 2. POST /search
    if (req.method === "POST" && url.pathname === "/search") {
      const body = await req.json();
      const { query } = body;
      if (!query) return new Response("Missing query", { status: 400 });

      // Basic implementation: update immediately.
      // Later, you will move this to an async batcher to prevent write-blocking!
      trie.insert(query, 1);

      return Response.json({ success: true, message: "Search recorded" });
    }

    // 3. Serve Frontend (Basic static file serving)
    if (req.method === "GET" && url.pathname === "/") {
      const file = Bun.file("./public/index.html");
      return new Response(file);
    }

    // 4. Serve Frontend Script (Basic static file serving)
    if (req.method === "GET" && url.pathname === "/script.js") {
      const file = Bun.file("./public/script.js");
      return new Response(file);
    }

    return new Response("Not Found", { status: 404 });
  },
});

console.log(`🦊 Typeahead server running at http://localhost:${server.port}`);
