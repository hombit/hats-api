/* What this page does beyond being markup, and none of it is load-bearing: the listing
   is complete and correct with the script removed.

   A file this service reads as data gets a panel that asks it a question. The columns
   come from the file when the panel is opened rather than with the listing: a directory
   of ten thousand partitions would otherwise pay a footer read per entry to describe
   files nobody asked about. Nothing here builds an <a href>, because a client scraping
   this page for entries reads the markup and would take one for a file. */

/* How many rows the table below the panel shows. It is a preview of the file, not the
   answer to a question about it: the answer is what the download and the url give. */
const PREVIEW = 10;

/* How much of one value the table keeps. A nested column holds a whole light curve, and
   one row of five thousand points is two hundred kilobytes of text — ten of those is a
   table the browser lays out for a second and nobody reads. The cell keeps a glance, the
   tooltip keeps enough to tell two of them apart, and the file keeps the rest: the url
   beside the button is where the whole value is. A type is cut for the same reason —
   twelve levels of nesting spell out to a paragraph. */
const CELL = 120;
const TOOLTIP = 1000;
const TYPE = 200;

function cut(text, limit) {
  return text.length > limit ? text.slice(0, limit) + '…' : text;
}

/* One of a thing is one column, one row: a count is written out with the word that goes
   with it rather than with an `s` that is wrong as often as it is right. */
function counted(count, one, many) {
  return count + ' ' + (count === 1 ? one : many);
}

document.body.classList.add('js');

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
  /* Name, button, size, time — the panel is under all four. */
  cell.colSpan = 4;
  cell.innerHTML =
    '<div class="panel-body">' +
    '<div class="columns-of"><span class="count">reading the columns…</span>' +
    '<input class="find" placeholder="find a column" hidden><div class="chips"></div></div>' +
    '<label><span>columns</span><textarea class="columns" rows="1" ' +
    'placeholder="all of them"></textarea></label>' +
    '<label><span>filters</span><textarea class="filters" rows="1" ' +
    'placeholder="every row"></textarea></label>' +
    '<span class="buttons">' +
    '<button class="run preview">Preview ' + PREVIEW + ' rows</button>' +
    '<a class="run download">Download parquet</a></span>' +
    '<div class="asked"></div><div class="result"></div></div>';
  row.dataset.url = url;
  cell.querySelector('.preview').addEventListener('click', () => run(row));
  /* A predicate over several columns is long enough to want more than a line, so Enter
     writes one and the modifier runs the query — the shape every editor and console
     already uses for a box you can type a newline into. */
  for (const field of cell.querySelectorAll('textarea')) {
    field.addEventListener('keydown', event => {
      if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        run(row);
      }
    });
    /* The download is a link, so its address has to be right before it is clicked
       rather than worked out on the way. */
    field.addEventListener('input', () => address(row));
  }
  address(row);
  return row;
}

/* Where the download link points, from the fields as they read now. It takes no limit:
   asking for parquet is asking for the rows that matched. */
function address(panel) {
  panel.querySelector('.download').href =
    url(panel.dataset.url, {...asked(panel), format: 'parquet'});
}

/* What columns the file has, which is one query with no rows in it. The answer's schema
   is the file's own, since this asks for no projection. */
function describe(panel) {
  const count = panel.querySelector('.count');
  const chips = panel.querySelector('.chips');
  ask(panel.dataset.url, {limit: '0', format: 'json'})
    .then(answer => {
      count.textContent = counted(answer.schema.length, 'column', 'columns') + ':';
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
  button.title = cut(column.type, TYPE);
  button.addEventListener('click', () => {
    const columns = panel.querySelector('.columns');
    const names = listed(columns.value);
    const spelled = written(column.name);
    const at = names.indexOf(spelled);
    if (at === -1) {
      names.push(spelled);
    } else {
      names.splice(at, 1);
    }
    columns.value = names.join(', ');
  });
  return button;
}

/* A name only stands for itself in SQL when SQL would read it as a name, and a file's
   columns are whatever the file calls them: `E(BP-RP)`, `column with spaces`, `μ_α*`.
   The one that matters is a name with a comma in it — written bare it does not fail, it
   silently asks for two other columns, which may well both exist. */
const BARE = /^[A-Za-z_][A-Za-z0-9_]*$/;

function written(name) {
  return BARE.test(name) ? name : '"' + name.replace(/"/g, '""') + '"';
}

/* The projection as it currently reads, split on the commas that separate names rather
   than on the ones inside them. */
function listed(value) {
  const names = [];
  let name = '';
  let quoted = false;
  for (const character of value) {
    if (character === '"') {
      quoted = !quoted;
      name += character;
    } else if (character === ',' && !quoted) {
      names.push(name.trim());
      name = '';
    } else {
      name += character;
    }
  }
  names.push(name.trim());
  return names.filter(Boolean);
}

/* What the fields say, as query parameters. Written over several lines and sent as one:
   a newline is nothing to SQL but noise in a url, and this url is shown, copied and
   linked to. */
function asked(panel) {
  const value = selector => panel.querySelector(selector).value.replace(/\s+/g, ' ').trim();
  return {columns: value('.columns'), filters: value('.filters')};
}

function run(panel) {
  const parameters = {
    ...asked(panel),
    /* The table here is a preview, so it is the front of the file and stays that size
       whatever the query. */
    limit: String(PREVIEW),
    /* `format` says what comes back, and it is not the `Accept` header: the file server
       answers a query in the format the url named, defaulting to parquet. Asking without
       it and reading the body as JSON parses a parquet file. */
    format: 'json',
  };
  show(panel.querySelector('.asked'), url(panel.dataset.url, parameters));
  const result = panel.querySelector('.result');
  result.textContent = 'running…';
  ask(panel.dataset.url, parameters)
    .then(answer => render(result, answer))
    .catch(error => fail(result, error));
}

/* The url this query is, as something to open, copy or send to someone. It is a link the
   script makes rather than one the page was served with: an <a href> below this directory
   in the markup would be scraped as an entry, and this one points at the same file with a
   question on it. */
function show(into, asked) {
  into.textContent = '';
  const link = document.createElement('a');
  link.href = asked;
  link.textContent = asked;
  link.target = '_blank';
  link.rel = 'noopener';
  into.appendChild(link);
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
  /* A full preview is the front of the file rather than the whole answer, and the two
     read alike at a glance — so it says which it is. */
  summary.textContent =
    (answer.num_rows === PREVIEW
      ? 'first ' + counted(PREVIEW, 'row', 'rows')
      : counted(answer.num_rows, 'row', 'rows')) +
    ', ' + bytes(answer.data_bytes_read) + ' read, ' + answer.elapsed_ms + ' ms';
  into.appendChild(summary);
  if (answer.rows.length === 0) return;

  const table = document.createElement('table');
  const head = table.insertRow();
  for (const column of answer.schema) {
    const cell = document.createElement('th');
    cell.textContent = column.name;
    cell.title = cut(column.type, TYPE);
    head.appendChild(cell);
  }
  for (const row of answer.rows) {
    const line = table.insertRow();
    for (const column of answer.schema) {
      const value = row[column.name];
      /* A HATS row can hold a whole light curve in one column, so a value that is not
         scalar is shown as what it is rather than as [object Object]. */
      const shown =
        value === undefined || value === null ? '' :
        typeof value === 'object' ? JSON.stringify(value) : String(value);
      const cell = line.insertCell();
      cell.textContent = cut(shown, CELL);
      if (shown.length > CELL) {
        cell.title = cut(shown, TOOLTIP) + '\n\n' + shown.length + ' characters';
      }
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
