// Frontend (Phase 4, Step 9)
// Vanilla JS typeahead: 150ms debounced /suggest calls + a /trending section.
// All requests go to the Load Balancer (same origin), which routes by hash.

const input = document.getElementById("q");
const suggestionsEl = document.getElementById("suggestions");
const trendingEl = document.getElementById("trending");
const metaEl = document.getElementById("meta");

const DEBOUNCE_MS = 150;
let activeIndex = -1;
let currentSuggestions = [];

// --- debounce helper: collapse rapid keystrokes into one request ----------
function debounce(fn, wait) {
  let t;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), wait);
  };
}

function escapeHtml(s) {
  return s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

// Bold the matched prefix at the start of each suggestion. Slice the RAW text
// on character boundaries first, then escape each piece (escaping first and
// slicing by raw length would split multi-char HTML entities like &amp;).
function highlight(text, prefix) {
  if (prefix && text.toLowerCase().startsWith(prefix.toLowerCase())) {
    const matched = escapeHtml(text.slice(0, prefix.length));
    const rest = escapeHtml(text.slice(prefix.length));
    return `<mark>${matched}</mark>${rest}`;
  }
  return escapeHtml(text);
}

function renderSuggestions(prefix, suggestions, shard) {
  currentSuggestions = suggestions;
  activeIndex = -1;
  suggestionsEl.innerHTML = suggestions
    .map(
      (s, i) =>
        `<li role="option" data-i="${i}" data-q="${escapeHtml(s)}">` +
        `<span class="ico">↗</span><span>${highlight(s, prefix)}</span></li>`,
    )
    .join("");
  metaEl.textContent = suggestions.length
    ? `${suggestions.length} suggestions · served by shard ${shard ?? "?"}`
    : prefix
      ? "no suggestions"
      : "";
}

const fetchSuggestions = debounce(async (prefix) => {
  if (!prefix) {
    renderSuggestions("", []);
    return;
  }
  try {
    const res = await fetch(`/suggest?q=${encodeURIComponent(prefix)}`);
    const data = await res.json();
    // Ignore stale responses if the input changed while in flight.
    if (input.value.trim() === prefix) {
      renderSuggestions(prefix, data.suggestions ?? [], data.shard);
    }
  } catch (err) {
    metaEl.textContent = "suggest request failed";
  }
}, DEBOUNCE_MS);

input.addEventListener("input", () => fetchSuggestions(input.value.trim()));

// Keyboard navigation through the suggestion list.
input.addEventListener("keydown", (e) => {
  const items = [...suggestionsEl.querySelectorAll("li")];
  if (e.key === "ArrowDown" && items.length) {
    e.preventDefault();
    activeIndex = (activeIndex + 1) % items.length;
  } else if (e.key === "ArrowUp" && items.length) {
    e.preventDefault();
    activeIndex = (activeIndex - 1 + items.length) % items.length;
  } else if (e.key === "Enter") {
    const chosen = activeIndex >= 0 ? currentSuggestions[activeIndex] : input.value.trim();
    if (chosen) submitSearch(chosen);
    return;
  } else {
    return;
  }
  items.forEach((li, i) => li.setAttribute("aria-selected", i === activeIndex));
});

suggestionsEl.addEventListener("click", (e) => {
  const li = e.target.closest("li");
  if (li) submitSearch(li.dataset.q);
});

// Record a completed search (POST /search → 202) then refresh trending.
async function submitSearch(query) {
  input.value = query;
  renderSuggestions(query, []);
  metaEl.textContent = `searched "${query}"`;
  try {
    await fetch("/search", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query }),
    });
  } catch {
    /* fire-and-forget; the server buffers asynchronously */
  }
  loadTrending();
}

async function loadTrending() {
  try {
    const res = await fetch("/trending");
    const data = await res.json();
    trendingEl.innerHTML = (data.trending ?? [])
      .map(
        (t, i) =>
          `<li data-q="${escapeHtml(t.query)}">` +
          `<span class="rank">${i + 1}</span>` +
          `<span>${escapeHtml(t.query)}</span>` +
          `<span class="score">${Math.round(t.score).toLocaleString()}</span></li>`,
      )
      .join("");
  } catch {
    trendingEl.innerHTML = "";
  }
}

trendingEl.addEventListener("click", (e) => {
  const li = e.target.closest("li");
  if (li) {
    input.value = li.dataset.q;
    fetchSuggestions(li.dataset.q);
    input.focus();
  }
});

// Trending on page load (Step 9).
loadTrending();
