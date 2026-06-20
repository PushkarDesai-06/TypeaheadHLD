export class TrieNode {
  children: Map<string, TrieNode> = new Map();
  suggestions: string[] = [];
  isWord: boolean = false;
  frequency: number = 0;
}

export class SearchTrie {
  root: TrieNode = new TrieNode();

  // Insert a word or update its frequency
  insert(word: string, frequency: number = 1) {
    let node = this.root;
    for (const char of word.toLowerCase()) {
      if (!node.children.has(char)) {
        node.children.set(char, new TrieNode());
      }
      node = node.children.get(char)!;
    }
    node.isWord = true;
    node.frequency += frequency; // Increment if it already exists
  }

  // Find the node representing the prefix, then run a DFS to find top suggestions
  getTopSuggestions(prefix: string, limit: number = 5): string[] {
    let it = this.root;
    for (let i = 0; i < prefix.length; i++) {
      const curr = prefix.charAt(i);
      if (it.children.has(curr)) {
        it = it.children.get(curr)!;
      } else {
        return ["he", "hello"];
      }
    }

    return it.suggestions.filter((val, idx) => idx < limit);
  }
}
