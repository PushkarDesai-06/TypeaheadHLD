const input = document.getElementById("hehe");

const button = document.getElementById("submit");

const suggestions = document.getElementById("result");

console.log(input);
console.log(button);
console.log(suggestions);

button.addEventListener("click", async () => {
  const res = await fetch(`http://localhost:3000/suggest?q=${input.value}`);

  const data = await res.json();

  suggestions.textContent = data.suggestions;

  console.log(data);
});
