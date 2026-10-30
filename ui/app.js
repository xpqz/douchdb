/* douchdb console - Vue 3, no build step.
 *
 * Every request goes through `api()`, which talks only to the public CouchDB
 * HTTP API and records the method, path, status, timing, request JSON and
 * response JSON for the optional request log.
 */
(() => {
  'use strict';

  const { createApp, ref, reactive, computed, onMounted } = Vue;

  class ApiError extends Error {
    constructor(result) {
      const data = result.data || {};
      super(data.reason || data.error || `HTTP ${result.status}`);
      this.name = 'ApiError';
      this.status = result.status;
      this.data = data;
    }
  }

  const enc = encodeURIComponent;

  function uuid() {
    if (crypto.randomUUID) return crypto.randomUUID();
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
  }

  function defaultBase() {
    if (location.protocol.startsWith('http') && location.pathname.startsWith('/_utils')) {
      return location.origin;
    }
    return 'http://127.0.0.1:5984';
  }

  function pretty(value) {
    if (value === null || value === undefined) return '—';
    if (typeof value === 'string') return value;
    return JSON.stringify(value, null, 2);
  }

  function clip(text, limit = 50000) {
    return text.length > limit ? `${text.slice(0, limit)}\n… (truncated)` : text;
  }

  const App = {
    setup() {
      const baseUrl = ref(localStorage.getItem('douchdb.ui.base') || defaultBase());
      const databases = ref([]);
      const newDb = ref('');
      const currentDb = ref(null);
      const view = ref('home'); // home | docs | doc | changes | info

      const docs = ref([]);
      const totalRows = ref(0);
      const page = ref(0);
      const pageSize = ref(20);
      const search = ref('');
      const activeSearch = ref('');

      const editor = reactive({
        id: '',
        isNew: false,
        rev: null,
        deleted: false,
        text: '{}',
        error: '',
        status: '',
      });

      const changes = ref([]);
      const changesLastSeq = ref(0);
      const follow = ref(false);

      const requests = ref([]);
      const selectedLog = ref(null);
      const logOpen = ref(false);

      const loginOpen = ref(false);
      const loginName = ref('admin');
      const loginPass = ref('');
      const loginError = ref('');
      const user = ref(null);

      const serverInfo = ref(null);
      const membership = ref(null);
      const dbInfo = ref(null);

      const toast = ref('');
      const toastKind = ref('ok');

      let logId = 0;
      let toastId = 0;
      let changesBusy = false;

      const apiRoot = computed(() => baseUrl.value.replace(/\/+$/, ''));
      const pageCount = computed(() =>
        Math.max(1, Math.ceil(totalRows.value / pageSize.value))
      );

      // ----- plumbing ----------------------------------------------------

      function showToast(message, kind = 'ok') {
        toast.value = message;
        toastKind.value = kind;
        const id = ++toastId;
        setTimeout(() => {
          if (id === toastId) toast.value = '';
        }, 3500);
      }

      function statusClass(status) {
        if (status === 0) return 'bad';
        if (status >= 200 && status < 300) return 'ok';
        if (status === 401 || status === 403) return 'warn';
        return 'bad';
      }

      async function api(method, path, body) {
        const started = performance.now();
        const headers = {};
        let requestText = '';
        if (body !== undefined) {
          requestText = clip(pretty(body));
          headers['Content-Type'] = 'application/json';
        }

        let result;
        try {
          const response = await fetch(apiRoot.value + path, {
            method,
            headers,
            credentials: 'include',
            body: body === undefined ? undefined : JSON.stringify(body),
          });
          const text = await response.text();
          let data = null;
          if (text) {
            try {
              data = JSON.parse(text);
            } catch {
              data = text;
            }
          }
          result = { ok: response.ok, status: response.status, data, text };
        } catch (error) {
          result = {
            ok: false,
            status: 0,
            data: { error: 'network_error', reason: String(error) },
            text: String(error),
          };
        }

        requests.value.unshift({
          id: ++logId,
          method,
          path,
          status: result.status,
          ok: result.ok,
          ms: Math.round(performance.now() - started),
          request: requestText,
          response: clip(typeof result.data === 'string' ? result.data : pretty(result.data)),
        });
        if (requests.value.length > 200) requests.value.pop();

        if (result.status === 401) {
          user.value = null;
          openLogin();
        }
        return result;
      }

      async function call(method, path, body) {
        const result = await api(method, path, body);
        if (!result.ok) throw new ApiError(result);
        return result.data;
      }

      function handleError(error, prefix) {
        if (error && error.status === 401) {
          openLogin();
          return;
        }
        const message = error && error.data
          ? error.data.reason || error.data.error || error.message
          : error.message || String(error);
        showToast(prefix ? `${prefix}: ${message}` : message, 'danger');
      }

      // ----- boot and routing --------------------------------------------

      function saveBase() {
        baseUrl.value = baseUrl.value.trim().replace(/\/+$/, '') || defaultBase();
        localStorage.setItem('douchdb.ui.base', baseUrl.value);
        databases.value = [];
        currentDb.value = null;
        serverInfo.value = null;
        membership.value = null;
        view.value = 'home';
        refreshAll();
      }

      async function refreshSession() {
        try {
          const data = await call('GET', '/_session');
          const authenticated = data && data.info && data.info.authenticated;
          const name = data && data.userCtx && data.userCtx.name;
          user.value = authenticated && name && name !== 'null' ? name : null;
        } catch {
          user.value = null;
        }
      }

      async function loadDbs() {
        try {
          databases.value = await call('GET', '/_all_dbs');
        } catch (error) {
          databases.value = [];
          if (error.status !== 401) handleError(error, 'Cannot list databases');
        }
      }

      async function loadServerInfo() {
        try {
          serverInfo.value = await call('GET', '/');
          membership.value = await call('GET', '/_membership');
        } catch (error) {
          if (error.status !== 401) handleError(error, 'Cannot read server info');
        }
      }

      async function refreshAll() {
        await refreshSession();
        if (user.value) await loadDbs();
        await loadServerInfo();
        if (currentDb.value && view.value !== 'doc') applyRoute();
      }

      function dbHash(name, sub) {
        return `#/db/${enc(name)}/${sub}`;
      }

      function docHash(name, id) {
        return dbHash(name, `doc/${enc(id)}`);
      }

      function navigate(hash) {
        if (location.hash === hash) applyRoute();
        else location.hash = hash;
      }

      function applyRoute() {
        const raw = location.hash.replace(/^#\/?/, '');
        if (!raw) {
          view.value = 'home';
          return;
        }
        const parts = raw.split('/');
        if (parts[0] !== 'db' || !parts[1]) {
          view.value = 'home';
          return;
        }
        const name = decodeURIComponent(parts[1]);
        if (name !== currentDb.value) {
          currentDb.value = name;
          docs.value = [];
          totalRows.value = 0;
          page.value = 0;
          search.value = '';
          activeSearch.value = '';
          changes.value = [];
          changesLastSeq.value = 0;
          dbInfo.value = null;
        }
        if (parts[2] === 'doc' && parts[3]) {
          loadDocById(decodeURIComponent(parts.slice(3).join('/')));
        } else if (parts[2] === 'new' && parts[3]) {
          startNewDoc(decodeURIComponent(parts.slice(3).join('/')));
        } else if (parts[2] === 'changes') {
          view.value = 'changes';
          if (!changes.value.length) loadNow();
        } else if (parts[2] === 'info') {
          view.value = 'info';
          loadDbInfo();
        } else {
          view.value = 'docs';
          loadDocs();
        }
      }

      // ----- databases ---------------------------------------------------

      async function openDb(name) {
        currentDb.value = name;
        page.value = 0;
        search.value = '';
        activeSearch.value = '';
        navigate(dbHash(name, 'docs'));
      }

      async function createDb() {
        const name = newDb.value.trim();
        if (!name) return;
        try {
          await call('PUT', `/${enc(name)}`, {});
          newDb.value = '';
          await loadDbs();
          showToast(`Created ${name}`);
          openDb(name);
        } catch (error) {
          handleError(error, 'Cannot create database');
        }
      }

      async function deleteDb() {
        const name = currentDb.value;
        if (!name || !confirm(`Delete database "${name}" and all its documents?`)) return;
        try {
          await call('DELETE', `/${enc(name)}`);
          showToast(`Deleted ${name}`);
          currentDb.value = null;
          databases.value = databases.value.filter((db) => db !== name);
          view.value = 'home';
          location.hash = '';
          await loadDbs();
        } catch (error) {
          handleError(error, 'Cannot delete database');
        }
      }

      async function loadDbInfo() {
        if (!currentDb.value) return;
        try {
          dbInfo.value = await call('GET', `/${enc(currentDb.value)}`);
        } catch (error) {
          handleError(error, 'Cannot read database info');
        }
      }

      async function compactDb() {
        try {
          await call('POST', `/${enc(currentDb.value)}/_compact`, {});
          showToast('Compaction requested');
          await loadDbInfo();
        } catch (error) {
          handleError(error, 'Cannot compact database');
        }
      }

      // ----- documents ---------------------------------------------------

      async function loadDocs() {
        if (!currentDb.value) return;
        const query = [];
        if (activeSearch.value) {
          query.push(
            `startkey=${enc(JSON.stringify(activeSearch.value))}`,
            `endkey=${enc(JSON.stringify(activeSearch.value + '\ufff0'))}`,
            'limit=100',
            'include_docs=false'
          );
        } else {
          query.push(
            `limit=${pageSize.value}`,
            `skip=${page.value * pageSize.value}`,
            'include_docs=false'
          );
        }
        try {
          const data = await call(
            'GET',
            `/${enc(currentDb.value)}/_all_docs?${query.join('&')}`
          );
          docs.value = data.rows || [];
          totalRows.value = data.total_rows || 0;
        } catch (error) {
          docs.value = [];
          handleError(error, 'Cannot list documents');
        }
      }

      function searchDocs() {
        activeSearch.value = search.value.trim();
        page.value = 0;
        loadDocs();
      }

      function clearSearch() {
        search.value = '';
        activeSearch.value = '';
        page.value = 0;
        loadDocs();
      }

      function nextPage() {
        if (page.value + 1 < pageCount.value) {
          page.value += 1;
          loadDocs();
        }
      }

      function prevPage() {
        if (page.value > 0) {
          page.value -= 1;
          loadDocs();
        }
      }

      function backToDocs() {
        navigate(dbHash(currentDb.value, 'docs'));
      }

      async function loadDocById(id) {
        try {
          const doc = await call('GET', `/${enc(currentDb.value)}/${enc(id)}`);
          const keepStatus =
            editor.id === (doc._id || id) && editor.rev === (doc._rev || null);
          editor.id = doc._id || id;
          editor.rev = doc._rev || null;
          editor.deleted = Boolean(doc._deleted);
          editor.text = pretty(doc);
          editor.isNew = false;
          editor.error = '';
          if (!keepStatus) editor.status = '';
          view.value = 'doc';
        } catch (error) {
          handleError(error, 'Cannot open document');
          if (error.status === 404) backToDocs();
        }
      }

      function startNewDoc(id) {
        editor.id = id;
        editor.rev = null;
        editor.deleted = false;
        editor.isNew = true;
        editor.text = '{\n  \n}';
        editor.error = '';
        editor.status = '';
        view.value = 'doc';
      }

      function newDoc() {
        const id = uuid();
        navigate(dbHash(currentDb.value, `new/${enc(id)}`));
      }

      function openDoc(id) {
        navigate(docHash(currentDb.value, id));
      }

      function formatDoc() {
        try {
          editor.text = pretty(JSON.parse(editor.text));
          editor.error = '';
        } catch (error) {
          editor.error = `Invalid JSON: ${error.message}`;
        }
      }

      async function saveDoc() {
        editor.error = '';
        editor.status = '';
        const id = editor.id.trim();
        if (!id) {
          editor.error = 'Document id is required.';
          return;
        }
        let doc;
        try {
          doc = JSON.parse(editor.text);
        } catch (error) {
          editor.error = `Invalid JSON: ${error.message}`;
          return;
        }
        if (doc === null || typeof doc !== 'object' || Array.isArray(doc)) {
          editor.error = 'The document body must be a JSON object.';
          return;
        }
        doc._id = id;
        if (editor.rev) doc._rev = editor.rev;
        try {
          const result = await call(
            'PUT',
            `/${enc(currentDb.value)}/${enc(id)}`,
            doc
          );
          editor.rev = result.rev;
          editor.deleted = false;
          editor.status = `Saved ${result.rev}`;
          showToast(`Saved ${id}`);
          await loadDocs();
          await loadDbInfo();
          if (editor.isNew) {
            editor.isNew = false;
            location.hash = docHash(currentDb.value, id);
          }
        } catch (error) {
          editor.error = error.data
            ? pretty(error.data)
            : error.message || String(error);
          handleError(error, 'Cannot save document');
        }
      }

      async function deleteDoc() {
        if (editor.isNew) {
          backToDocs();
          return;
        }
        if (!confirm(`Delete document "${editor.id}"?`)) return;
        try {
          await call(
            'DELETE',
            `/${enc(currentDb.value)}/${enc(editor.id)}?rev=${enc(editor.rev)}`
          );
          showToast(`Deleted ${editor.id}`);
          await loadDocs();
          await loadDbInfo();
          backToDocs();
        } catch (error) {
          handleError(error, 'Cannot delete document');
        }
      }

      // ----- changes feed -------------------------------------------------

      async function loadChanges(since, limit) {
        if (!currentDb.value || changesBusy) return;
        changesBusy = true;
        try {
          const path =
            `/${enc(currentDb.value)}/_changes?since=${enc(String(since))}` +
            `&limit=${limit}&include_docs=true`;
          const data = await call('GET', path);
          const rows = (data.results || []).slice().reverse();
          changes.value = rows.concat(changes.value).slice(0, 300);
          changesLastSeq.value = data.last_seq;
        } catch (error) {
          handleError(error, 'Cannot read changes');
          follow.value = false;
        } finally {
          changesBusy = false;
        }
      }

      function loadNow() {
        changes.value = [];
        loadChanges('now', 100);
      }

      function loadHistory() {
        changes.value = [];
        loadChanges(0, 100);
      }

      function openChanges() {
        navigate(dbHash(currentDb.value, 'changes'));
      }

      function openInfo() {
        navigate(dbHash(currentDb.value, 'info'));
      }

      function seqLabel(seq) {
        if (seq === null || seq === undefined) return '';
        const text = String(seq);
        return text.length > 16 ? `${text.slice(0, 14)}…` : text;
      }

      // ----- login -------------------------------------------------------

      function openLogin() {
        loginOpen.value = true;
        loginError.value = '';
      }

      async function doLogin() {
        loginError.value = '';
        try {
          await call('POST', '/_session', {
            name: loginName.value,
            password: loginPass.value,
          });
          user.value = loginName.value;
          loginPass.value = '';
          loginOpen.value = false;
          showToast(`Signed in as ${user.value}`);
          await loadDbs();
          await loadServerInfo();
        } catch (error) {
          loginError.value =
            error.data && error.data.reason
              ? error.data.reason
              : error.message || 'Sign in failed';
        }
      }

      async function logout() {
        try {
          await call('DELETE', '/_session');
        } catch {
          /* the cookie is client-side; ignore servers that do not implement it */
        }
        user.value = null;
        databases.value = [];
        currentDb.value = null;
        view.value = 'home';
        location.hash = '';
        showToast('Signed out');
        await loadDbs();
        await loadServerInfo();
      }

      // ----- request log -------------------------------------------------

      function toggleLog(id) {
        selectedLog.value = selectedLog.value === id ? null : id;
      }

      async function copyLog(entry) {
        const text = [
          `${entry.method} ${entry.path} -> ${entry.status} (${entry.ms} ms)`,
          '',
          'Request:',
          entry.request || '(no body)',
          '',
          'Response:',
          entry.response,
        ].join('\n');
        try {
          await navigator.clipboard.writeText(text);
          showToast('Request copied');
        } catch {
          showToast('Clipboard is not available', 'danger');
        }
      }

      // ----- lifecycle ---------------------------------------------------

      onMounted(async () => {
        window.addEventListener('hashchange', applyRoute);
        // When the console is hosted away from douchdb there is no way to
        // guess the API origin. Wait for the user to enter one in the header
        // instead of firing requests at the default and logging CORS errors.
        const servedByApi =
          location.protocol.startsWith('http') && location.pathname.startsWith('/_utils');
        const hasSavedServer = Boolean(localStorage.getItem('douchdb.ui.base'));
        if (!servedByApi && !hasSavedServer) return;

        await refreshSession();
        // Avoid a guaranteed 401 (and an error in the browser console) when
        // the server requires authentication and no session exists yet.
        if (user.value) await loadDbs();
        await loadServerInfo();
        applyRoute();
        setInterval(() => {
          if (follow.value && view.value === 'changes') {
            loadChanges(changesLastSeq.value, 100);
          }
        }, 2000);
      });

      return {
        baseUrl,
        databases,
        newDb,
        currentDb,
        view,
        docs,
        totalRows,
        page,
        pageSize,
        pageCount,
        search,
        activeSearch,
        editor,
        changes,
        follow,
        requests,
        selectedLog,
        logOpen,
        loginOpen,
        loginName,
        loginPass,
        loginError,
        user,
        serverInfo,
        membership,
        dbInfo,
        toast,
        toastKind,
        pretty,
        saveBase,
        refreshAll,
        loadDbs,
        loadServerInfo,
        openDb,
        createDb,
        deleteDb,
        loadDbInfo,
        compactDb,
        loadDocs,
        searchDocs,
        clearSearch,
        nextPage,
        prevPage,
        backToDocs,
        newDoc,
        openDoc,
        formatDoc,
        saveDoc,
        deleteDoc,
        openChanges,
        openInfo,
        loadNow,
        loadHistory,
        seqLabel,
        openLogin,
        doLogin,
        logout,
        toggleLog,
        copyLog,
        statusClass,
      };
    },
  };

  createApp(App).mount('#app');
})();
