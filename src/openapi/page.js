// Sends a request from the page and shows what came back. Everything below is an addition to
// markup that is already complete: with the script off, every route, field and type is still
// on the page, and the `curl` line still says what to send.
//
// Nothing is stored. No history, no remembered values, no localStorage — a request here may
// carry a credential, and the way to not keep one is to not write it down.
(function () {
  document.body.classList.add('js');

  // The origin the browser actually reached this service by, which the server rendering the
  // page does not know: it knows the path it serves, not the name in front of it.
  var origin = window.location.origin;

  // A route linked to from the contents is collapsed until it is opened, so following the link
  // would otherwise land on a closed row that looks like every other closed row.
  function openTarget() {
    if (!window.location.hash) return;
    var target = document.getElementById(window.location.hash.slice(1));
    if (target && target.tagName === 'DETAILS') target.open = true;
  }
  openTarget();
  window.addEventListener('hashchange', openTarget);

  document.querySelectorAll('form.try').forEach(function (form) {
    var path = origin + form.dataset.path;
    var body = form.querySelector('.body');
    var answer = form.querySelector('.answer');
    var asCurl = form.querySelector('.as-curl');

    function shell() {
      return (
        "curl -sS -X POST '" + path + "' \\\n" +
        "  -H 'content-type: application/json' \\\n" +
        "  -d '" + body.value.replace(/'/g, "'\\''") + "'"
      );
    }
    asCurl.textContent = shell();
    body.addEventListener('input', function () {
      asCurl.textContent = shell();
    });

    form.addEventListener('submit', function (event) {
      event.preventDefault();
      var button = form.querySelector('.send');
      button.disabled = true;
      answer.hidden = false;
      answer.textContent = 'sending…';
      var started = performance.now();

      fetch(path, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: body.value,
      })
        .then(function (response) {
          var elapsed = Math.round(performance.now() - started);
          // A parquet answer is bytes, and its counts travel as headers because the body has
          // no room for them. Printing the bytes would fill the page with mojibake.
          var type = response.headers.get('content-type') || '';
          if (type.indexOf('json') === -1) {
            return response.blob().then(function (blob) {
              var counts = ['num-rows', 'data-bytes-read', 'elapsed-ms']
                .map(function (name) {
                  var value = response.headers.get('x-hats-' + name);
                  return value === null ? null : name + '=' + value;
                })
                .filter(Boolean)
                .join('  ');
              return (
                response.status + ' ' + type + ', ' + blob.size + ' bytes' +
                (counts ? '\n' + counts : '')
              );
            });
          }
          return response.text().then(function (text) {
            var shown = text;
            try {
              shown = JSON.stringify(JSON.parse(text), null, 2);
            } catch (error) {
              // Not JSON after all; show what arrived rather than nothing.
            }
            return response.status + '  ' + elapsed + ' ms\n\n' + shown;
          });
        })
        .catch(function (error) {
          return 'could not send: ' + error;
        })
        .then(function (text) {
          answer.textContent = text;
          button.disabled = false;
        });
    });
  });
})();
