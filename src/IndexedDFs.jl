module IndexedDFs

import DataFrames

struct IndexedDF
    df::DataFrames.DataFrame
    index_col::String
    index_col_i::Int
end

function IndexedDF(df::DataFrames.DataFrame, index_col)
    index_col = String(index_col)

    # Validate the index column exists
    if !(index_col in names(df))
        throw(ArgumentError(
            "Index column $(index_col) does not exist in DataFrame"
        ))
    end

    # Validate uniqueness of index column
    col = df[!, index_col]
    if length(unique(col)) != length(col)
        throw(ArgumentError(
            "Index column $(index_col) contains duplicate values"
        ))
    end

    # Save index column position
    index_col_i = findfirst(==(index_col), names(df))

    return IndexedDF(df, index_col, index_col_i)
end

# Find the index in the underlying DataFrame corresponding to a given idx_val
function find_row(idf::IndexedDF, idx_val)
    row_i = findfirst(==(idx_val), idf.df[!, idf.index_col])
    if row_i === nothing
        throw(KeyError("No row with index $(idx_val)"))
    end
    return row_i
end

# Verify that setindex isn't duplicating an already existing index value
function check_setindex(
        idf::IndexedDF,
        val,
        idx_val,
        col::Union{String, Symbol})
    is_setting_index = String(col) == idf.index_col
    current_val = idf[idx_val, col]
    val_is_changing = current_val != val
    already_exists = val in idf.df[!, idf.index_col]
    if (
        is_setting_index
        && val_is_changing
        && already_exists
    )
        throw(ArgumentError(
            "The index column already contains a $val row."
        ))
    end
    return nothing
end

###############################################################################
# Overloads
###############################################################################
import Base: getindex, setindex!, show, getproperty, deleteat!, push!

# Allow getting a row by the value in the index column:
function getindex(idf::IndexedDF, idx_val)
    row_i = find_row(idf, idx_val)
    return idf.df[row_i, :]
end

# Allow getting a whole column
function getindex(idf::IndexedDF, ::Colon, col::Union{String, Symbol})
    return idf.df[:, col]
end

# Allow getting a specific cell by (index_val, column)
function getindex(idf::IndexedDF, idx_val, col)
    col = String(col)
    row_i = find_row(idf, idx_val)
    return idf.df[row_i, col]
end

# For updating a single cell (value can be Int, Float64, String, etc.)
function setindex!(idf::IndexedDF, value, idx_val, col::Union{String, Symbol})
    row_i = find_row(idf, idx_val)
    check_setindex(idf, value, idx_val, col)
    idf.df[row_i, col] = value
    return idf
end

# Allow setting a whole row by index_val with a NamedTuple or Dict
function setindex!(idf::IndexedDF, row_data::Union{NamedTuple, Dict}, idx_val)
    row_i = find_row(idf, idx_val)
    for (col, val) in pairs(row_data)
        check_setindex(idf, val, idx_val, col)
        idf.df[row_i, col] = val
    end
    return idf
end

# Show method for nicer display
function show(io::IO, idf::IndexedDF)
    print(io, "IndexedDF with index column $(idf.index_col):\n")
    show(io, idf.df)
end

# Allow the user to retrieve properties from the idf.df like `idf.df.col` via
# idf.col
function getproperty(idf::IndexedDF, name::Symbol)
    if name in fieldnames(IndexedDF)
        return getfield(idf, name)
    else
        return getproperty(idf.df, name)
    end
end

# Allow the user to delete a row by specifying the idx_val
function deleteat!(idf::IndexedDF, idx_val)
    row_i = find_row(idf, idx_val)
    deleteat!(idf.df, row_i)
    return idf
end

# Allow the user to add a row to the IndexedDF via a NamedTuple or Dict
function push!(idf::IndexedDF, row::Union{NamedTuple, Dict})
    # The key for a NamedTuple must be a Symbol.
    key = isa(row, NamedTuple) ? Symbol(idf.index_col) : idf.index_col
    index_val = row[key]
    if index_val in idf.df[!, idf.index_col]
        throw(ArgumentError("Duplicate index value: $index_val"))
    end
    push!(idf.df, row)
    return idf
end

# Allow the user to add a row to the IndexedDF via a Vector
function push!(idf::IndexedDF, row::Vector)
    if length(row) != DataFrames.ncol(idf.df)
        throw(ArgumentError("Row has wrong number of columns"))
    end
    index_val = row[idf.index_col_i]
    if index_val in idf.df[!, idf.index_col]
        throw(ArgumentError("Duplicate index value: $index_val"))
    end
    push!(idf.df, row)
    return idf
end

# Allow the user to add a column
function setindex!(
            idf::IndexedDF,
            col::AbstractVector,
            ::Colon,
            col_name::Union{String, Symbol}
        )
    if length(col) != DataFrames.nrow(idf.df)
        throw(ArgumentError("Column has the wrong number of rows."))
    end
    idf.df[:, col_name] = col
end

end # module
