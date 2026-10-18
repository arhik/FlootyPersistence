module FlootyPersistence

using FlootyAbstract
using FlootyAbstract: AbstractPersistenceBackend, AbstractPersistable, save!, load, delete!, list
using StructTypes
using JSON3
using UUIDs
using Dates
import SQLite
import DBInterface

export JSONBackend
export save!, load, delete!, list
export SQLiteJournal, JournalEvent, open_journal, close!, journal_path,
    append_event!, append_events!, journal_events, journal_sessions,
    journal_doctor, schema_version, upcast_row, meta_get, meta_set!,
    JOURNAL_SCHEMA_VERSION, JOURNAL_MIN_READER, ensure_schema!

include("json.jl")
include("journal.jl")

end # module FlootyPersistence
