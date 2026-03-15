module FlootyPersistence

using FlootyAbstract
using FlootyAbstract: AbstractPersistenceBackend, AbstractPersistable, save!, load, delete!, list
using StructTypes
using JSON3
using UUIDs
using Dates

export JSONBackend
export save!, load, delete!, list

include("json.jl")

end # module FlootyPersistence
