# journal.jl — the append-only event journal.
#
# One row per thing that happened (instruction, assistant text/reasoning, tool
# call/result, eval outcome, session lifecycle). SQLite is the hot store today;
# Arrow shards are the durable long-term store (see
# FlootyArchitecture/JOURNAL_AND_INDEX_STORE.md). The schema and the readers are
# written so the storage under them can change without touching callers.
#
# Two rules the rest of the family relies on:
#   * `session_uid` is minted ONCE per session and never changes — a session is a
#     projection (`filter(events, session_uid == uid)`), not a file.
#   * `schema_version` is a COLUMN, authoritative per row. Rows older than the
#     reader are upcast; rows newer than the reader raise (never silently misread).

const JOURNAL_SCHEMA_VERSION = 1
const JOURNAL_MIN_READER = 1

# Versioned DDL. Append a new entry per schema change — never edit an applied one:
# an existing database replays only the versions it is missing.
const JOURNAL_MIGRATIONS = Tuple{Int,Vector{String}}[
    (1, [
        """
        CREATE TABLE IF NOT EXISTS journal_meta(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL)""",
        """
        CREATE TABLE IF NOT EXISTS journal_migrations(
            version INTEGER PRIMARY KEY,
            ddl TEXT NOT NULL,
            applied_at REAL NOT NULL)""",
        """
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_wall REAL NOT NULL,
            ts_mono REAL NOT NULL,
            session_uid TEXT NOT NULL,
            terminal_id TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT '',
            content TEXT,
            thinking TEXT,
            tool_name TEXT,
            tool_call_id TEXT,
            ok INTEGER,
            model TEXT NOT NULL DEFAULT '',
            agent TEXT NOT NULL DEFAULT '',
            cwd TEXT NOT NULL DEFAULT '',
            payload TEXT,
            schema_version INTEGER NOT NULL DEFAULT $(JOURNAL_SCHEMA_VERSION))""",
        "CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_uid, id)",
        "CREATE INDEX IF NOT EXISTS idx_events_kind ON events(kind, id)",
        "CREATE INDEX IF NOT EXISTS idx_events_time ON events(ts_wall)",
    ]),
]

"One journal row, normalized to the CURRENT schema (see `upcast_row`)."
struct JournalEvent
    id::Int64
    ts_wall::Float64
    ts_mono::Float64
    session_uid::String
    terminal_id::String
    kind::String
    role::String
    content::Union{Nothing,String}
    thinking::Union{Nothing,String}
    tool_name::Union{Nothing,String}
    tool_call_id::Union{Nothing,String}
    ok::Union{Nothing,Bool}
    model::String
    agent::String
    cwd::String
    payload::Union{Nothing,String}
    schema_version::Int16
end

"A live journal handle: one SQLite connection, one writer, WAL."
struct SQLiteJournal
    db::SQLite.DB
    path::String
    lock::ReentrantLock
end

journal_path(j::SQLiteJournal) = j.path

# ---- plumbing ---------------------------------------------------------------

"Run a statement and FREE its cursor (SQLite refuses COMMIT with one in progress)."
function exec!(j::SQLiteJournal, sql::AbstractString, params = ())
    cur = DBInterface.execute(j.db, sql, params)
    DBInterface.close!(cur)
    nothing
end

"Run a query, materializing rows while the forward-only cursor is valid."
function query_all(j::SQLiteJournal, sql::AbstractString, params, mapper)
    out = Any[]
    cur = DBInterface.execute(j.db, sql, params)
    try
        for row in cur
            push!(out, mapper(row))
        end
    finally
        DBInterface.close!(cur)
    end
    return out
end

"Applied schema version of this database (`PRAGMA user_version`)."
function schema_version(j::SQLiteJournal)::Int
    return lock(j.lock) do
        Int(first(first(DBInterface.execute(j.db, "PRAGMA user_version"))))
    end
end

"Journal-level metadata (schema version, migrator checkpoint, …)."
function meta_get(j::SQLiteJournal, key::AbstractString, default = nothing)
    return lock(j.lock) do
        rows = query_all(j, "SELECT value FROM journal_meta WHERE key = ?", (String(key),),
                         r -> String(r.value))
        isempty(rows) ? default : rows[1]
    end
end

function meta_set!(j::SQLiteJournal, key::AbstractString, value::AbstractString)
    lock(j.lock) do
        exec!(j, "INSERT INTO journal_meta(key, value) VALUES(?, ?) " *
                 "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
              (String(key), String(value)))
    end
    return nothing
end

"Create/upgrade the schema. Idempotent; refuses a database newer than this code."
function ensure_schema!(j::SQLiteJournal)
    lock(j.lock) do
        current = Int(first(first(DBInterface.execute(j.db, "PRAGMA user_version"))))
        if current > JOURNAL_SCHEMA_VERSION
            error("journal at $(j.path) is schema v$current, newer than this reader " *
                  "(v$JOURNAL_SCHEMA_VERSION) — upgrade flooty instead of downgrading the file")
        end
        current == JOURNAL_SCHEMA_VERSION && return j
        for (version, statements) in JOURNAL_MIGRATIONS
            version <= current && continue
            exec!(j, "BEGIN")
            try
                for ddl in statements
                    exec!(j, ddl)
                end
                exec!(j, "INSERT OR REPLACE INTO journal_migrations(version, ddl, applied_at) " *
                         "VALUES(?, ?, ?)",
                      (version, join(statements, ";\n"), time()))
                # PRAGMA cannot be parameterized; `version` is a literal Int here.
                exec!(j, "PRAGMA user_version = $(Int(version))")
                exec!(j, "COMMIT")
            catch err
                try exec!(j, "ROLLBACK") catch _ end
                rethrow(err)
            end
            current = version
        end
        meta_set_unlocked!(j, "schema_version", string(current))
        return j
    end
end

# `meta_set!` takes the lock; the schema upgrader already holds it.
function meta_set_unlocked!(j::SQLiteJournal, key::AbstractString, value::AbstractString)
    exec!(j, "INSERT INTO journal_meta(key, value) VALUES(?, ?) " *
             "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
          (String(key), String(value)))
    nothing
end

"""
    open_journal(path; create = true) :: SQLiteJournal

Open (or create) the journal at `path`. WAL + `synchronous=NORMAL`: a committed
event survives a crash, and one insert costs tens of microseconds.
"""
function open_journal(path::AbstractString; create::Bool = true)::SQLiteJournal
    p = String(path)
    create && mkpath(dirname(abspath(p)))
    db = SQLite.DB(p)
    j = SQLiteJournal(db, p, ReentrantLock())
    lock(j.lock) do
        for pragma in ("PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL",
                       "PRAGMA busy_timeout=5000")
            cur = DBInterface.execute(db, pragma)
            DBInterface.close!(cur)
        end
    end
    create && ensure_schema!(j)
    return j
end

function close!(j::SQLiteJournal)
    lock(j.lock) do
        try DBInterface.close!(j.db) catch _ end
    end
    return nothing
end

# ---- write path -------------------------------------------------------------

const _INSERT_EVENT = """
    INSERT INTO events(ts_wall, ts_mono, session_uid, terminal_id, kind, role,
                       content, thinking, tool_name, tool_call_id, ok,
                       model, agent, cwd, payload, schema_version)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

_as_string(x) = x === nothing ? nothing : String(x)

"""
    append_event!(j; kind, session_uid, …) :: Int64

Append one event; returns its id (the journal's global order). Rejects an empty
`kind`/`session_uid` — an unattributed event is a bug in the producer, not data.
`content` never carries reasoning: reasoning goes in `thinking`.
"""
function append_event!(j::SQLiteJournal; kind::AbstractString,
                       session_uid::AbstractString,
                       terminal_id::AbstractString = "",
                       role::AbstractString = "",
                       content = nothing, thinking = nothing,
                       tool_name = nothing, tool_call_id = nothing,
                       ok = nothing,
                       model::AbstractString = "", agent::AbstractString = "",
                       cwd::AbstractString = "",
                       payload = nothing,
                       ts_wall::Real = time(),
                       ts_mono::Real = time_ns() / 1e9,
                       schema_version::Integer = JOURNAL_SCHEMA_VERSION)::Int64
    isempty(strip(kind)) &&
        throw(ArgumentError("journal: kind must be non-empty"))
    isempty(strip(session_uid)) &&
        throw(ArgumentError("journal: session_uid must be non-empty (mint it per session)"))
    okval = ok === nothing ? nothing : (ok ? 1 : 0)
    lock(j.lock) do
        cur = DBInterface.execute(j.db, _INSERT_EVENT,
            (Float64(ts_wall), Float64(ts_mono), String(session_uid), String(terminal_id),
             String(kind), String(role), _as_string(content), _as_string(thinking),
             _as_string(tool_name), _as_string(tool_call_id), okval,
             String(model), String(agent), String(cwd), _as_string(payload),
             Int(schema_version)))
        DBInterface.close!(cur)
        return Int64(SQLite.last_insert_rowid(j.db))
    end
end

"""
    append_events!(j, rows) :: Int

Append many events in ONE transaction (importers, replay, tests). Each row is a
NamedTuple with the same keywords `append_event!` accepts. 50-row statements are
the measured sweet spot; bigger ones cost more, not less.
"""
function append_events!(j::SQLiteJournal, rows)
    isempty(rows) && return 0
    n = 0
    lock(j.lock) do
        exec!(j, "BEGIN")
        try
            stmt = DBInterface.prepare(j.db, _INSERT_EVENT)
            try
                for r in rows
                    okv = get(r, :ok, nothing)
                    DBInterface.execute(stmt,
                        (Float64(get(r, :ts_wall, time())),
                         Float64(get(r, :ts_mono, time_ns() / 1e9)),
                         String(r.session_uid), String(get(r, :terminal_id, "")),
                         String(r.kind), String(get(r, :role, "")),
                         _as_string(get(r, :content, nothing)),
                         _as_string(get(r, :thinking, nothing)),
                         _as_string(get(r, :tool_name, nothing)),
                         _as_string(get(r, :tool_call_id, nothing)),
                         okv === nothing ? nothing : (okv ? 1 : 0),
                         String(get(r, :model, "")), String(get(r, :agent, "")),
                         String(get(r, :cwd, "")),
                         _as_string(get(r, :payload, nothing)),
                         Int(get(r, :schema_version, JOURNAL_SCHEMA_VERSION))))
                    n += 1
                end
            finally
                DBInterface.close!(stmt)
            end
            exec!(j, "COMMIT")
        catch err
            try exec!(j, "ROLLBACK") catch _ end
            rethrow(err)
        end
    end
    return n
end

# ---- read path --------------------------------------------------------------

# Column order must match `JournalEvent`.
const _SELECT_EVENTS = """
    SELECT id, ts_wall, ts_mono, session_uid, terminal_id, kind, role,
           content, thinking, tool_name, tool_call_id, ok,
           model, agent, cwd, payload, schema_version
    FROM events"""

# SQLite hands NULL back as `missing` (not `nothing`) through a Row cursor.
_absent(x) = x === nothing || ismissing(x)
_opt_string(x) = _absent(x) ? nothing : String(x)
_opt_bool(x) = _absent(x) ? nothing : Bool(x)

row_to_event(r) = JournalEvent(
    Int64(r.id), Float64(r.ts_wall), Float64(r.ts_mono),
    String(r.session_uid), String(r.terminal_id), String(r.kind), String(r.role),
    _opt_string(r.content), _opt_string(r.thinking),
    _opt_string(r.tool_name), _opt_string(r.tool_call_id), _opt_bool(r.ok),
    String(r.model), String(r.agent), String(r.cwd),
    _opt_string(r.payload), Int16(r.schema_version))

"""
    upcast_row(ev::JournalEvent) -> JournalEvent

Normalize one row to `JOURNAL_SCHEMA_VERSION`. A row NEWER than this reader raises
(misreading a newer journal is worse than refusing it); so does a row older than
the supported window. Add one `_upcast(Val(v), ev)` per version hop — never edit an
existing hop, it is the only record of what v→v+1 meant.
"""
function upcast_row(ev::JournalEvent)::JournalEvent
    v = Int(ev.schema_version)
    v > JOURNAL_SCHEMA_VERSION &&
        error("journal row $(ev.id) is schema v$v, newer than this reader " *
              "(v$JOURNAL_SCHEMA_VERSION) — upgrade flooty")
    v < JOURNAL_MIN_READER &&
        error("journal row $(ev.id) is schema v$v, older than the supported window " *
              "(v$JOURNAL_MIN_READER)")
    while v < JOURNAL_SCHEMA_VERSION
        ev = _upcast(Val(v), ev)
        v += 1
    end
    return ev
end

# v1 is current: no hops yet. The chain exists so a future bump is one function
# plus a migration entry, and `test/journalProbe.jl` asserts every hop is present.
# (_upcast(::Val{1}, ev) = ev would be the first hop when v2 lands.)

"""
    journal_events(j; session_uid = "", kind = "", after_id = 0, limit = 0)
        :: Vector{JournalEvent}

Events in journal order, upcast to the current schema. Filters compose.
"""
function journal_events(j::SQLiteJournal; session_uid::AbstractString = "",
                        kind::AbstractString = "", after_id::Integer = 0,
                        limit::Integer = 0)::Vector{JournalEvent}
    where = String[]; params = Any[]
    if !isempty(session_uid)
        push!(where, "session_uid = ?"); push!(params, String(session_uid))
    end
    if !isempty(kind)
        push!(where, "kind = ?"); push!(params, String(kind))
    end
    if after_id > 0
        push!(where, "id > ?"); push!(params, Int64(after_id))
    end
    sql = _SELECT_EVENTS * (isempty(where) ? "" : " WHERE " * join(where, " AND ")) *
          " ORDER BY id"
    limit > 0 && (sql *= " LIMIT $(Int(limit))")
    rows = lock(j.lock) do
        query_all(j, sql, Tuple(params), row_to_event)
    end
    return JournalEvent[upcast_row(ev) for ev in rows]
end

"""
    journal_sessions(j) :: Vector{NamedTuple}

One entry per `session_uid`, newest activity first — the basis of a session picker.
"""
function journal_sessions(j::SQLiteJournal)
    sql = """
        SELECT session_uid,
               MIN(ts_wall) AS first_ts,
               MAX(ts_wall) AS last_ts,
               COUNT(*) AS events,
               MAX(id) AS last_id,
               GROUP_CONCAT(DISTINCT CASE WHEN terminal_id <> '' THEN terminal_id END)
                   AS terminal_ids
        FROM events
        GROUP BY session_uid
        ORDER BY last_id DESC"""
    return lock(j.lock) do
        query_all(j, sql, (), r -> (
            session_uid = String(r.session_uid),
            first_ts = Float64(r.first_ts),
            last_ts = Float64(r.last_ts),
            events = Int(r.events),
            last_id = Int64(r.last_id),
            terminal_ids = _absent(r.terminal_ids) ? String[] :
                           String.(split(String(r.terminal_ids), ",")),
        ))
    end
end

"Row count, schema version and file size — what a `journal doctor` prints."
function journal_doctor(j::SQLiteJournal)
    rows = lock(j.lock) do
        Int(first(first(DBInterface.execute(j.db, "SELECT COUNT(*) AS n FROM events"))))
    end
    return (path = j.path,
            schema_version = schema_version(j),
            reader_version = JOURNAL_SCHEMA_VERSION,
            rows = rows,
            sessions = length(journal_sessions(j)),
            bytes = isfile(j.path) ? filesize(j.path) : 0)
end
