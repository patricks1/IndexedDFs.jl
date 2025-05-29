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

import Base: getindex, setindex!, show, getproperty

# Allow getting a row by the value in the index column:
function getindex(idf::IndexedDF, idx_val)
    # Find row where index_col == idx_val
    row_i = findfirst(==(idx_val), idf.df[!, idf.index_col])
    if row_i === nothing
        throw(KeyError("No row with index $(idx_val)"))
    end
    return idf.df[row_i, :]
end

# Allow getting a specific cell by (index_val, column)
function getindex(idf::IndexedDF, idx_val, col)
    column = String(column)

    row_i = findfirst(==(idx_val), idf.df[!, idf.index_col])
    if row_i === nothing
        throw(KeyError("No row with index $(idx_val)"))
    end
    return idf.df[row_i, col]
end

# For updating a single cell (value can be Int, Float64, String, etc.)
function setindex!(idf::IndexedDF, value, idx_val, col)
    row_i = findfirst(==(idx_val), idf.df[!, idf.index_col])
    if row_i === nothing
        throw(KeyError("No row with index $(idx_val)"))
    end
    idf.df[row_i, col] = value
    return idf
end

# Allow setting a whole row by index_val with a NamedTuple or Dict
function setindex!(idf::IndexedDF, row_data::Union{NamedTuple, Dict}, idx_val)
    row_i = findfirst(==(idx_val), idf.df[!, idf.index_col])
    
    if row_i === nothing
        throw(KeyError("No row with index $(idx_val)"))
    end
    println(row_data)
    for (k,v) in pairs(row_data)
        idf.df[row_i, k] = v
    end
    # Optional: re-check index uniqueness if you allow changing index_col 
    # values here
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

end # module
