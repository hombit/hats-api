/* Tabs over the boxes the markup already holds. Without this every box is shown under its
   name; with it, a bar per example and one choice for the page — someone who reads pyvo
   under one example wants pyvo under the next. */
document.body.classList.add('js');

let chosen = 'ADQL';

function show() {
  for (const tab of document.querySelectorAll('.client-tab')) {
    tab.classList.toggle('on', tab.dataset.client === chosen);
  }
  for (const code of document.querySelectorAll('.client-code')) {
    code.hidden = code.dataset.client !== chosen;
  }
}

function copy(button) {
  const code = button.closest('.clients').querySelector('.client-code:not([hidden])');
  navigator.clipboard.writeText(code.textContent).then(
    () => {
      button.textContent = 'copied';
      setTimeout(() => (button.textContent = 'copy'), 1200);
    },
    () => (button.textContent = 'press ⌘C'),
  );
}

document.addEventListener('click', event => {
  const tab = event.target.closest('.client-tab');
  if (tab) {
    chosen = tab.dataset.client;
    show();
    return;
  }
  const button = event.target.closest('.client-copy');
  if (button) copy(button);
});

show();
