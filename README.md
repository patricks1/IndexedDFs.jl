# IndexedDFs
A small package to allow Julia DataFrames to have meaningful row indices.  

Create an `IndexedDF` by feeding a `DataFrames.DataFrame` into `IndexedDFs.IndexedDF` and specifying the index column. For example,
```julia
import DataFrames
import IndexedDFs
df = DataFrames.DataFrame(id=[100, 200, 300], a=[1, 2, 3], b=[4, 5, 6])
idf = IndexedDFs.IndexedDF(df, "id")
```
The `IndexedDF` then behaves much like a regular `DataFrames.DataFrame`, although I haven't made an exhaustive effort to cover all the overloads.  

Be cautions about manually modifying the underlying `DataFrame`. Things like `IndexedDFs.check_uniqueness` don't run when you do that until the next time you access the `IndexedDF`.
