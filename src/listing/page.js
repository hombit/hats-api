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
    '<div class="asked"></div>' +
    clients() +
    '<div class="result"></div></div>';
  row.dataset.url = url;
  cell.querySelector('.preview').addEventListener('click', () => run(row));
  for (const tab of cell.querySelectorAll('.client-tab')) {
    tab.addEventListener('click', () => choose(row, tab.dataset.client));
  }
  const copy = cell.querySelector('.client-copy');
  if (copy) copy.addEventListener('click', () => copied(row));
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
       rather than worked out on the way. The snippets are read and copied, which is the
       same requirement. */
    field.addEventListener('input', () => address(row));
  }
  address(row);
  return row;
}

/* The API's own subtree, or null when API mode is off — in which case there is no
   request to write and the panel does not offer one. */
const API = document.body.dataset.api || null;

/* Which of the snippets was last looked at. One choice for the page rather than one per
   panel: someone who writes Python opens the next file's panel wanting Python. */
let client = 'curl';

/* Each snippet asks the same question, and they differ in what they hand back and in how
   the file is named. Two shapes:

   - a POST to the API, whose body names the file as `file://` and this page's own path.
     Anything that reads bytes takes this one.
   - a GET on the file's own url with the query on it, for a reader that takes a url and
     fetches it. Not every reader can: a query answer is generated rather than served off
     the disk, so it carries no `accept-ranges`, and one that reads parquet by asking for
     the footer's byte range refuses it outright rather than reading the whole body.

   `pip` names, not import names — `nested-pandas` is installed with a hyphen and
   imported with an underscore, and the line is there to be pasted. Alphabetical, because
   the order says nothing and one that drifts is one more thing to read. `aiohttp` is what
   `fsspec` fetches a url with, and is named because nothing else pulls it in. */
const CLIENTS = {
  curl: {},
  requests: {pip: 'requests', write: viaRequests},
  nested_pandas: {pip: 'aiohttp nested-pandas requests', write: viaNestedPandas},
  astropy: {pip: 'astropy pyarrow requests', write: viaAstropy},
  pyarrow: {pip: 'pyarrow requests', write: viaPyarrow},
};

function clients() {
  if (API === null) return '';
  return (
    '<div class="clients"><div class="client-tabs">' +
    Object.keys(CLIENTS)
      .map(
        name =>
          '<button class="client-tab" data-client="' + name + '">' + name + '</button>'
      )
      .join('') +
    '<button class="client-copy" title="Copy to the clipboard">copy</button>' +
    '</div><pre class="client-code"></pre></div>'
  );
}

function choose(panel, name) {
  client = name;
  snippet(panel);
}

/* The request this query is, as a client would send it. The file is named by this page's
   own path with `file://` in front — a mount's `path` is its address in both modes, so
   there is nothing here to look up and nothing that says where the file is on the disk.

   Built as text rather than as a link: an <a href> below this directory in the markup is
   an entry to a client scraping the page. */
function snippet(panel) {
  const code = panel.querySelector('.client-code');
  if (!code) return;
  for (const tab of panel.querySelectorAll('.client-tab')) {
    tab.classList.toggle('on', tab.dataset.client === client);
  }
  const route = new URL(API.replace(/\/$/, '') + '/parquet', location.href).href;
  const body = {url: 'file://' + panel.dataset.url};
  const {columns, filters} = asked(panel);
  if (columns !== '') body.columns = columns;
  if (filters !== '') body.filters = filters;
  /* The file's own url with the query on it, which is the other way to ask the same
     question — and the one a reader that takes a url can be handed directly. */
  const got = new URL(
    url(panel.dataset.url, {...asked(panel), format: 'parquet'}),
    location.href
  ).href;
  code.textContent = write(route, body, got);
}

function write(route, body, got) {
  const {pip, write: writer} = CLIENTS[client];
  if (writer === undefined) return curl(route, body);
  return '# pip install ' + pip + '\n' + writer(route, body, got);
}

/* Single-quoted, so the shell leaves the JSON alone; `format` is asked for by name
   because the API answers JSON by default and this is the shape a shell can read. */
function curl(route, body) {
  return (
    'curl -sS -X POST ' + route + ' \\\n' +
    "  -H 'content-type: application/json' \\\n" +
    '  -d ' + quoted(JSON.stringify({...body, format: 'json'}))
  );
}

function viaRequests(route, body) {
  return (
    'import requests\n\n' +
    post(route, body) +
    'rows = answer.json()["rows"]'
  );
}

/* Parquet back, and read without touching the disk: the answer is a file, and the point
   of asking for it is to keep the types the JSON body spells as text. */
function viaPyarrow(route, body) {
  return (
    'import io\n\nimport pyarrow.parquet as pq\nimport requests\n\n' +
    post(route, {...body, format: 'parquet'}) +
    'table = pq.read_table(io.BytesIO(answer.content))'
  );
}

/* The file's own url is the whole request: `read_parquet` takes one and `fsspec` fetches
   it, so there is no client to write. A HATS row holds a whole light curve in one column,
   and `nested_pandas` is what reads that as a frame rather than as a column of lists. */
function viaNestedPandas(route, body, got) {
  return 'import nested_pandas as npd\n\nframe = npd.read_parquet(\n    ' + text(got) + '\n)';
}

/* Through `pyarrow` rather than `Table.read`, which needs `pandas` for a parquet file
   whatever else is installed. The columns go across as a dict, so nothing here is a
   second copy of the types. */
function viaAstropy(route, body) {
  return (
    'import io\n\nimport pyarrow.parquet as pq\nimport requests\nfrom astropy.table import Table\n\n' +
    post(route, {...body, format: 'parquet'}) +
    'table = Table(pq.read_table(io.BytesIO(answer.content)).to_pydict())'
  );
}

/* The POST every reader above makes, and the `raise_for_status` that turns a refusal into
   an exception rather than into a parse error further down. */
function post(route, body) {
  return (
    'answer = requests.post(\n' +
    '    ' + text(route) + ',\n' +
    '    json=' + python(JSON.stringify(body, null, 4)) + ',\n' +
    ')\nanswer.raise_for_status()\n'
  );
}

/* One argument to the shell, whatever is in it. A single-quoted string ends at the first
   quote, and `band = 'g'` is an ordinary predicate here — so a quote is closed, escaped
   and reopened, which is the only thing a single-quoted shell string cannot carry. */
function quoted(text) {
  return "'" + text.replace(/'/g, "'\\''") + "'";
}

/* A Python string literal. `JSON.stringify` escapes the quote and the backslash the same
   way Python does, and a url has nothing else in it that either language reads. */
function text(value) {
  return JSON.stringify(value);
}

/* JSON is very nearly a Python literal, and the difference here is only the indentation:
   the body has no `true`, `false` or `null` in it, since every value is a string this
   panel put there. */
function python(json) {
  return json.replace(/\n/g, '\n    ');
}

function copied(panel) {
  const code = panel.querySelector('.client-code');
  const button = panel.querySelector('.client-copy');
  navigator.clipboard.writeText(code.textContent).then(
    () => {
      button.textContent = 'copied';
      setTimeout(() => (button.textContent = 'copy'), 1200);
    },
    () => (button.textContent = 'press ⌘C')
  );
}

/* Where the download link points, from the fields as they read now. It takes no limit:
   asking for parquet is asking for the rows that matched. */
function address(panel) {
  panel.querySelector('.download').href =
    url(panel.dataset.url, {...asked(panel), format: 'parquet'});
  snippet(panel);
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
    /* Setting `value` from a script fires no `input` event, so the download link and the
       snippets would go on describing the query as it was before the chip was clicked. */
    address(panel);
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
      const cell = line.insertCell();
      const value = row[column.name];
      /* The values that are not a measurement are set apart from the ones that are, so a
         column of numbers does not have a blank in it that reads as nothing much. `null`
         is the file saying it has no value here, whatever the column holds. `NaN` and the
         infinities are values the file does hold, and arrive as strings because JSON has
         no number for them — so they count only in a float column, where a string cannot
         be anything else. A `Utf8` column whose value really is the text "NaN" is a
         string like any other. */
      const special =
        value === null || value === undefined ? 'null'
        : column.type.startsWith('Float') &&
          (value === 'NaN' || value === 'Infinity' || value === '-Infinity') ? value
        : null;
      if (special !== null) {
        cell.textContent = special;
        cell.className = 'special';
        continue;
      }
      /* A HATS row can hold a whole light curve in one column, so a value that is not
         scalar is shown as what it is rather than as [object Object]. */
      const shown =
        value === undefined || value === null ? '' :
        typeof value === 'object' ? JSON.stringify(value) : String(value);
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
