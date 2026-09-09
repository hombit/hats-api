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
  describe(panel, panel.dataset.url);
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
  const {route, body} = request(panel);
  /* The same question as a url with the query on it, which is the other way to ask it —
     and the one a reader that takes a url can be handed directly. */
  const got = new URL(
    url(panel.dataset.url, {...asked(panel), format: 'parquet'}),
    location.href
  ).href;
  code.textContent = write(route, body, got);
}

/* The API request a panel's fields are, as a route and a body. Two routes over one body
   shape: a file names itself and a catalog names itself and a shape on the sky, and the
   column names are the catalog's own to answer — which is why they appear in neither. */
function request(panel) {
  const {ra, dec, radius_arcsec, ...rest} = asked(panel);
  const catalog = panel.dataset.catalog !== undefined;
  const body = {url: 'file://' + panel.dataset.url, ...rest};
  if (catalog) {
    /* Numbers, not the strings the fields hold: the body is JSON, and `"45.6"` is a string
       where a coordinate is expected. An empty field leaves the region out entirely, so a
       half-written circle is not sent as one. */
    const circle = [ra, dec, radius_arcsec].map(Number);
    if (circle.every(value => Number.isFinite(value))) {
      body.region = [{type: 'circle', ra: circle[0], dec: circle[1], radius_arcsec: circle[2]}];
    } else {
      /* With nothing to narrow it, the request is the whole catalog, which the API refuses
         as readily as this url does. The limit is what makes it the front of the catalog
         instead — so the snippet carries the one the panel just used, and is a request that
         runs rather than one that comes back as a plan. */
      body.limit = PREVIEW;
    }
  }
  return {
    route: new URL(API.replace(/\/$/, '') + (catalog ? '/hats' : '/parquet'), location.href).href,
    body: body,
  };
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
   asking for parquet is asking for the rows that matched.

   The url shown beside it follows the fields too, rather than only the last query that ran.
   It is the thing on this panel someone copies into a client or sends to a colleague, and
   one that lags the fields describes a different question than the one on the screen. */
function address(panel) {
  panel.querySelector('.download').href =
    url(panel.dataset.url, {...asked(panel), format: 'parquet'});
  const into = panel.querySelector('.asked');
  /* Half a circle is not a request, and a url written from one would answer a question
     nobody asked. Every other state has a url worth showing. */
  if (state(panel) === 'partial') {
    into.textContent = '';
  } else {
    show(into, url(panel.dataset.url, preview(panel)));
  }
  snippet(panel);
}

/* What the circle currently is, which is what decides which buttons mean anything.

   `none` is a request in its own right — the front of the catalog, in the catalog's own
   order — so the circle narrows an answer rather than being what makes one possible. A
   file's panel has no circle and is always `none`. */
function state(panel) {
  const {ra, dec, radius_arcsec} = asked(panel);
  const written = [ra, dec, radius_arcsec].filter(value => value !== undefined);
  if (written.length === 0) return 'none';
  if (written.length < 3 || !written.every(value => Number.isFinite(Number(value)))) {
    return 'partial';
  }
  return Number(radius_arcsec) > MAX_RADIUS ? 'wide' : 'ok';
}

/* What columns there are, which is one query with no rows in it. The answer's schema is
   the file's own, since this asks for no projection.

   `at` is what to ask: the file itself for a file's panel, and for a catalog the schema
   file the server pointed at — `dataset/_common_metadata`, which is every partition's
   columns and no rows. A catalog has no schema of its own to read, and asking one of its
   partitions would mean choosing one, which is the search. */
function describe(panel, at) {
  const count = panel.querySelector('.count');
  ask(at, {limit: '0', format: 'json'})
    .then(answer => chipsFrom(panel, answer.schema))
    .catch(error => fail(count.parentElement, error));
}

/* The columns as buttons. */
function chipsFrom(panel, schema) {
  const count = panel.querySelector('.count');
  const chips = panel.querySelector('.chips');
  /* An empty schema is not a file with no columns: it is a search that reached no
     partition, and the answer carries nothing to describe. Writing "0 columns" over the
     list would report the emptiness of the answer as a fact about the catalog. */
  if (chips.children.length > 0 || schema.length === 0) return;
  count.textContent = counted(schema.length, 'column', 'columns') + ':';
  for (const column of schema) {
    chips.appendChild(chip(panel, column));
  }
  /* A survey catalog runs to a couple of hundred columns, which is more than anyone
     reads down. Past a screenful the list gets a box of its own to scroll in and
     something to search it with. */
  if (schema.length > 12) {
    const find = panel.querySelector('.find');
    find.hidden = false;
    find.addEventListener('input', () => {
      const wanted = find.value.trim().toLowerCase();
      for (const button of chips.children) {
        button.hidden = !button.textContent.toLowerCase().includes(wanted);
      }
    });
  }
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
   linked to.

   Every field a panel might have, and a panel has the ones it has: the catalog's carries a
   circle and a file's does not. An empty one is left out rather than sent empty, since
   `filters=` is a predicate that parses as nothing. */
const FIELDS = ['ra', 'dec', 'radius_arcsec', 'columns', 'filters'];

function asked(panel) {
  const parameters = {};
  for (const name of FIELDS) {
    const field = panel.querySelector('.' + name);
    if (field === null) continue;
    const value = field.value.replace(/\s+/g, ' ').trim();
    if (value !== '') parameters[name] = value;
  }
  return parameters;
}

/* What the preview asks for, which is also what the url beside it shows. */
function preview(panel) {
  return {
    ...asked(panel),
    /* The table here is a preview, so it is the front of the file and stays that size
       whatever the query. */
    limit: String(PREVIEW),
    /* `format` says what comes back, and it is not the `Accept` header: the file server
       answers a query in the format the url named, defaulting to parquet. Asking without
       it and reading the body as JSON parses a parquet file. */
    format: 'json',
  };
}

function run(panel) {
  const parameters = preview(panel);
  const result = panel.querySelector('.result');
  result.textContent = 'running…';
  ask(panel.dataset.url, parameters)
    .then(answer => {
      chipsFrom(panel, answer.schema);
      render(result, answer);
    })
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
    /* A catalog says how many of its partitions were read, which is the number that says
       whether the region pruned; a file has one and does not mention it. */
    (answer.num_partitions === undefined
      ? ''
      : ' from ' + counted(answer.num_partitions, 'partition', 'partitions')) +
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

/* The catalog this directory belongs to, and the widest circle its url will answer. Both
   are the server's, read off the body rather than written into the script: one of them is
   configuration and the other is where the walk up from this directory landed. */
const CATALOG = document.body.dataset.catalog || null;
const MAX_RADIUS = Number(document.body.dataset.maxRadius);

/* Where this catalog's columns can be read, or null for a catalog that has no such file —
   in which case the chips arrive with the first answer instead, since every answer carries
   its schema. */
const SCHEMA = document.body.dataset.schema || null;

if (CATALOG !== null) catalog();

/* The cone search over the whole catalog, as a form.

   Always open, unlike a file's: a directory has one catalog and the search over it is the
   reason someone is looking at this page. The prose it replaces stays in the markup for a
   page whose script did not run — it says the same thing as a url, which is the part that
   works without any of this. */
function catalog() {
  const section = document.querySelector('section.catalog');
  if (section === null) return;
  const panel = document.createElement('div');
  panel.className = 'panel-body';
  /* Both, because the shared helpers read `url` to build a request and `catalog` to decide
     which kind of request it is. */
  panel.dataset.url = CATALOG;
  panel.dataset.catalog = '';
  /* The same order a file's panel is in — the columns to pick from, the query, the
     buttons, the url, a client, the answer — with the circle where the query begins. One
     panel read twice a day should not be laid out two ways. */
  panel.innerHTML =
    '<div class="columns-of">' +
    '<span class="count">' + (SCHEMA === null ? '' : 'reading the columns…') + '</span>' +
    '<input class="find" placeholder="find a column" hidden><div class="chips"></div></div>' +
    '<label><span>ra</span><input class="ra" placeholder="deg"></label>' +
    '<label><span>dec</span><input class="dec" placeholder="deg"></label>' +
    '<label><span>radius</span><input class="radius_arcsec" placeholder="arcsec"></label>' +
    '<label><span>columns</span><textarea class="columns" rows="1" ' +
    'placeholder="all of them"></textarea></label>' +
    '<label><span>filters</span><textarea class="filters" rows="1" ' +
    'placeholder="every row"></textarea></label>' +
    '<span class="buttons">' +
    '<button class="run preview">Preview ' + PREVIEW + ' rows</button>' +
    '<a class="run download">Download parquet</a>' +
    (API === null ? '' : '<button class="run plan">Plan</button>') +
    '</span>' +
    '<p class="gate"></p>' +
    '<div class="asked"></div>' +
    clients() +
    '<div class="result"></div>';
  section.appendChild(panel);

  panel.querySelector('.preview').addEventListener('click', () => run(panel));
  const plan = panel.querySelector('.plan');
  if (plan) plan.addEventListener('click', () => planned(panel));
  for (const tab of panel.querySelectorAll('.client-tab')) {
    tab.addEventListener('click', () => choose(panel, tab.dataset.client));
  }
  const copy = panel.querySelector('.client-copy');
  if (copy) copy.addEventListener('click', () => copied(panel));
  for (const field of panel.querySelectorAll('input.ra, input.dec, input.radius_arcsec, textarea')) {
    field.addEventListener('input', () => {
      address(panel);
      gate(panel);
    });
    field.addEventListener('keydown', event => {
      if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        run(panel);
      }
    });
  }
  address(panel);
  gate(panel);
  /* Before anyone asks anything, the way a file's panel does it: the columns are what
     someone writes a query out of, so they are of no use arriving with the answer. */
  if (SCHEMA !== null) describe(panel, SCHEMA);
}

/* What the fields as they stand will and will not answer, said before anything is asked
   rather than after.

   Four states and each button reads them differently. A preview always has something to
   show unless the circle is half-written or wider than the url answers. A download has no
   `limit` on it — asking for parquet is asking for the rows that matched — so with no
   circle it would be the whole catalog, which this url does not do. And the plan is the one
   thing a wide circle is still good for: it reads the catalog's own files and no rows, so it
   describes a search of any size. */
function gate(panel) {
  const at = state(panel);
  const note = panel.querySelector('.gate');
  note.textContent =
    at === 'partial' ? 'A cone search takes a centre and a radius.'
    : at === 'wide' ? 'Wider than ' + MAX_RADIUS + '″, which is more than one answer carries. Plan hands back the requests it fans out into.'
    : at === 'none' ? 'The front of the catalog, in its own order. A circle narrows it, and a download needs one.'
    : '';
  const off = {
    preview: at === 'partial' || at === 'wide',
    download: at !== 'ok',
    plan: at === 'partial',
  };
  for (const [name, dimmed] of Object.entries(off)) {
    const button = panel.querySelector('.' + name);
    if (button === null) continue;
    /* An anchor has no `disabled`, so the class is what all of them read. */
    button.classList.toggle('off', dimmed);
    if (button.disabled !== undefined) button.disabled = dimmed;
  }
}

/* The work this search fans out into, from the API's plan route: the requests a client would
   send, one per partition. It reads the catalog's own files and no rows, so it answers a
   search of any size — which is what it is here for. */
function planned(panel) {
  const {route, body} = request(panel);
  const into = panel.querySelector('.result');
  into.textContent = 'planning…';
  fetch(route + '/plan', {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify(body),
  })
    .then(response => response.text().then(text => ({response, body: parse(text)})))
    .then(({response, body}) => {
      if (!response.ok) throw new Error(body.error || response.status);
      into.textContent = '';
      const summary = document.createElement('p');
      summary.textContent =
        counted(body.num_partitions, 'partition', 'partitions') + ', ' +
        counted(body.requests.length, 'request', 'requests') +
        (body.requires_credentials ? ', each needing your storage options attached' : '');
      const written = document.createElement('pre');
      written.className = 'client-code';
      written.textContent = JSON.stringify(body, null, 2);
      into.appendChild(summary);
      into.appendChild(written);
    })
    .catch(error => fail(into, error));
}
