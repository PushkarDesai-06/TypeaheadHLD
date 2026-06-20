### **System Architecture & Execution Plan: Distributed Search Typeahead**

#### **Phase 1: Core Logic & Initial Seeding**

- **Step 1: Consistent Hashing Ring (`src/hash-ring.ts`)**
- Implement a consistent hashing algorithm (with virtual nodes for balanced distribution).
- Expose a deterministic routing function that takes a string (prefix or query) and outputs the target application node/shard (e.g., `app1`, `app2`, or `app3`).

- **Step 2: The Data Loader Script (`scripts/seed.ts`)**
- Parse the initial dataset (e.g., `data/search_frequencies.json`).
- For each search query, generate all valid prefixes.
- Pass each prefix through the consistent hashing function to determine its assigned Redis shard (`redis1`, `redis2`, or `redis3`).
- Use Redis Pipelines to bulk-load these precomputed prefix arrays into the appropriate shards before the application servers boot up.

#### **Phase 2: The Application Node & Batch Processing (`src/server.ts`)**

- **Step 3: Web Server & In-Memory Buffering**
- Initialize a stateless Bun HTTP server mapped 1:1 with its local Redis container.
- **`GET /suggest?q=<prefix>`:** Read directly from the Redis shard for low-latency retrieval.
- **`POST /search`:** Accept the final search query. Instead of writing to the database, push the query into a local, in-memory array (the buffer) and immediately return a `202 Accepted` to the client.

- **Step 4: The 100-Request Batch Writer**
- Implement a monitor on the in-memory buffer.
- The moment the buffer size hits **100 requests**, trigger the batch write protocol:

1. Extract and clear the buffer.
2. Generate every possible prefix for all 100 search queries.
3. Aggregate and deduplicate the counts locally in memory.
4. Open a single Redis Pipeline to update the frequencies of all these prefixes in the Redis database simultaneously.

#### **Phase 3: Trending Searches & Time Decay**

- **Step 5: Tracking Trends via ZSETs**
- During the batch write process, alongside updating the standard prefixes, push the completed search queries into a Redis Sorted Set (`ZSET`) designated for trending searches.
- Implement a **`GET /trending`** endpoint that queries this `ZSET` to return the highest-scoring search queries.

- **Step 6: The 10% Daily Decay Mechanism**
- Implement a scheduled cron job or a background worker loop that triggers once every 24 hours.
- This job must iterate through the trending `ZSET` and multiply every score by `0.9` (reducing the count by **10%**). This ensures that historically massive queries slowly lose dominance, allowing recent, highly active searches to rise to the top.

#### **Phase 4: Orchestration & UI**

- **Step 7: The Load Balancer (`src/lb.ts`)**
- Deploy a Bun-based reverse proxy.
- Intercept all incoming `/suggest` and `/search` requests, extract the query/prefix, and run it through the exact same consistent hashing logic used by the seeder script.
- Proxy the request to the dynamically selected Application Node. Log the routing decision for visibility.

- **Step 8: Docker Compose Topology**
- Provision the 7-container cluster: 1x Load Balancer, 3x Bun App Nodes, and 3x Redis Shards. Ensure strict networking rules so App Nodes only communicate with their designated Redis instance.

- **Step 9: Frontend (`public/index.html`)**
- Build a vanilla JS search interface with a **150ms debounce** on the keystroke event listener to prevent hammering the LB.
- Dynamically render both the autocomplete suggestions (via `/suggest`) as the user types, and a "Trending Right Now" section (via `/trending`) on page load.
