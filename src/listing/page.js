/* What this page does beyond being markup, and none of it is load-bearing: the listing
   is complete and correct with the script removed. Two things happen here.

   One, a timestamp is rewritten into the reader's own timezone, keeping the spelling the
   markup carries in the attribute and in the tooltip.

   Two, a file this service reads as data gets a panel that asks it a question. The
   columns come from the file when the panel is opened rather than with the listing: a
   directory of ten thousand partitions would otherwise pay a footer read per entry to
   describe files nobody asked about. Nothing here builds an <a href>, because a client
   scraping this page for entries reads the markup and would take one for a file. */

document.body.classList.add('js');

for (const stamp of document.querySelectorAll('time[datetime]')) {
  const at = new Date(stamp.dateTime);
  if (!isNaN(at.getTime())) {
    stamp.title = stamp.textContent;
    stamp.textContent = at.toLocaleString();
  }
}

for (const button of document.querySelectorAll('.ask')) {
  button.addEventListener('click', () => toggle(button));
}

function toggle(button) {
  const row = button.closest('tr');
  const open = row.nextElementSibling;
  if (open && open.classList.contains('panel')) {
    open.remove();
    return;
  }
  const panel = build(button.dataset.url);
  row.after(panel);
  describe(panel);
  panel.querySelector('.columns').focus();
}

/* The panel, built rather than written into every row: a `Dir=` level is thousands of
   entries and at most one of them is being asked about. */
function build(url) {
  const row = document.createElement('tr');
  row.className = 'panel';
  const cell = row.insertCell();
  cell.colSpan = 3;
  cell.innerHTML =
    '<div class="panel-body">' +
    '<div class="columns-of"><span class="count">reading the columns…</span>' +
    '<input class="find" placeholder="find a column" hidden><div class="chips"></div></div>' +
    '<label>columns <input class="columns" placeholder="all of them"></label>' +
    '<label>filters <input class="filters" placeholder="every row"></label>' +
    '<label>limit <input class="limit" type="number" min="0" value="10"></label>' +
    '<button class="run" data-format="json">Run</button>' +
    '<button class="run" data-format="parquet">Parquet</button>' +
    '<div class="asked"></div><div class="result"></div></div>';
  row.dataset.url = url;
  for (const button of cell.querySelectorAll('.run')) {
    button.addEventListener('click', () => run(row, button.dataset.format));
  }
  for (const input of cell.querySelectorAll('input')) {
    input.addEventListener('keydown', event => {
      if (event.key === 'Enter') run(row, 'json');
    });
  }
  return row;
}

/* What columns the file has, which is one query with no rows in it. The answer's schema
   is the file's own, since this asks for no projection. */
function describe(panel) {
  const count = panel.querySelector('.count');
  const chips = panel.querySelector('.chips');
  ask(panel.dataset.url, {limit: '0', format: 'json'})
    .then(answer => {
      count.textContent = answer.schema.length + ' columns:';
      for (const column of answer.schema) {
        chips.appendChild(chip(panel, column));
      }
      /* A survey catalog runs to a couple of hundred columns, which is more than anyone
         reads down. Past a screenful the list gets a box of its own to scroll in and
         something to search it with. */
      if (answer.schema.length > 12) {
        const find = panel.querySelector('.find');
        find.hidden = false;
        find.addEventListener('input', () => {
          const wanted = find.value.trim().toLowerCase();
          for (const button of chips.children) {
            button.hidden = !button.textContent.toLowerCase().includes(wanted);
          }
        });
      }
    })
    .catch(error => fail(count.parentElement, error));
}

/* A column, as a button that writes its own name into the projection. Astronomy column
   names are mixed-case and easy to mistype, and the service refuses a name it does not
   have rather than ignoring it. */
function chip(panel, column) {
  const button = document.createElement('button');
  button.className = 'chip';
  button.textContent = column.name;
  button.title = column.type;
  button.addEventListener('click', () => {
    const columns = panel.querySelector('.columns');
    const named = columns.value.split(',').map(name => name.trim()).filter(Boolean);
    const at = named.indexOf(column.name);
    if (at === -1) {
      named.push(column.name);
    } else {
      named.splice(at, 1);
    }
    columns.value = named.join(', ');
  });
  return button;
}

function run(panel, format) {
  const value = selector => panel.querySelector(selector).value.trim();
  const parameters = {
    columns: value('.columns'),
    filters: value('.filters'),
    limit: value('.limit'),
  };
  /* `format` says what comes back, and it is not the `Accept` header: the file server
     answers a query in the format the url named, defaulting to parquet. Asking without
     it and reading the body as JSON parses a parquet file. */
  parameters.format = format;
  const asked = url(panel.dataset.url, parameters);
  panel.querySelector('.asked').textContent = asked;
  if (format === 'parquet') {
    /* The browser saves it: the response is an attachment, so this navigates nowhere. */
    location.href = asked;
    return;
  }
  const result = panel.querySelector('.result');
  result.textContent = 'running…';
  ask(panel.dataset.url, parameters)
    .then(answer => render(result, answer))
    .catch(error => fail(result, error));
}

function url(path, parameters) {
  const query = new URLSearchParams();
  for (const [name, value] of Object.entries(parameters)) {
    if (value !== '') query.set(name, value);
  }
  const written = query.toString();
  return written === '' ? path : path + '?' + written;
}

/* Every request the panel makes, and the one place a refusal becomes an error: the
   service says what was wrong with a query in the body, and that sentence is the whole
   point of showing it. */
function ask(path, parameters) {
  return fetch(url(path, parameters))
    .then(response => response.text().then(text => ({response, body: parse(text)})))
    .then(({response, body}) => {
      if (!response.ok) throw new Error(body.error || response.status);
      return body;
    });
}

/* Every number in JavaScript is a double, so an id of more than fifteen digits — Gaia's
   `source_id` is nineteen — comes out of JSON.parse rounded, and the page would show an
   object id that does not exist. Nothing warns anyone: it is the right length and the
   wrong number. So a long integer is quoted before parsing and stays exact as text; the
   panel only ever displays it. The pattern cannot match inside a string value, since a
   quote there would be escaped. */
function parse(text) {
  return JSON.parse(text.replace(/("[^"\\]*"\s*:\s*)(-?\d{16,})(?=\s*[,}\]])/g, '$1"$2"'));
}

function render(into, answer) {
  into.textContent = '';
  const summary = document.createElement('p');
  summary.textContent =
    answer.num_rows + ' rows, ' + bytes(answer.data_bytes_read) + ' read, ' +
    answer.elapsed_ms + ' ms';
  into.appendChild(summary);
  if (answer.rows.length === 0) return;

  const table = document.createElement('table');
  const head = table.insertRow();
  for (const column of answer.schema) {
    const cell = document.createElement('th');
    cell.textContent = column.name;
    cell.title = column.type;
    head.appendChild(cell);
  }
  for (const row of answer.rows) {
    const line = table.insertRow();
    for (const column of answer.schema) {
      const value = row[column.name];
      /* A HATS row can hold a whole light curve in one column, so a value that is not
         scalar is shown as what it is rather than as [object Object]. */
      line.insertCell().textContent =
        value === undefined || value === null ? '' :
        typeof value === 'object' ? JSON.stringify(value) : value;
    }
  }
  into.appendChild(table);
}

function bytes(count) {
  const units = ['B', 'KiB', 'MiB', 'GiB'];
  let at = 0;
  while (count >= 1024 && at < units.length - 1) {
    count /= 1024;
    at += 1;
  }
  return (at === 0 ? count : count.toFixed(1)) + ' ' + units[at];
}

function fail(into, error) {
  into.textContent = '';
  const said = document.createElement('p');
  said.className = 'failed';
  said.textContent = error.message || String(error);
  into.appendChild(said);
}
