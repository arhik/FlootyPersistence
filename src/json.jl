# ═══════════════════════════════════════════════════════════════════════
# JSON Backend
# ═══════════════════════════════════════════════════════════════════════

struct JSONBackend <: AbstractPersistenceBackend
    base_path::String
end

JSONBackend() = JSONBackend(joinpath(homedir(), ".flooty", "sessions"))

function FlootyAbstract.save!(backend::JSONBackend, obj::T) where T <: AbstractPersistable
    mkpath(backend.base_path)
    id = FlootyAbstract.persistable_id(obj)
    filepath = joinpath(backend.base_path, "$(id).json")
    open(filepath, "w") do f
        JSON3.write(f, obj)
    end
    filepath
end

# `save!` keys the file by interpolating the id, so it already accepted any id;
# the read/delete halves were typed `::UUID`, which locked out String-keyed
# persistables (session ids in the PTY layer are strings). Widened to match.

function FlootyAbstract.load(backend::JSONBackend, ::Type{T},
                             id::Union{UUID,AbstractString}) where T <: AbstractPersistable
    filepath = joinpath(backend.base_path, "$(id).json")
    isfile(filepath) || return nothing
    JSON3.read(read(filepath, String), T)
end

function FlootyAbstract.delete!(backend::JSONBackend, ::Type{T},
                                id::Union{UUID,AbstractString}) where T <: AbstractPersistable
    filepath = joinpath(backend.base_path, "$(id).json")
    isfile(filepath) && rm(filepath)
    nothing
end

function FlootyAbstract.list(backend::JSONBackend, ::Type{T}) where T <: AbstractPersistable
    mkpath(backend.base_path)
    results = T[]
    for file in readdir(backend.base_path)
        endswith(file, ".json") || continue
        try
            obj = JSON3.read(read(joinpath(backend.base_path, file), String), T)
            push!(results, obj)
        catch
            continue
        end
    end
    results
end
