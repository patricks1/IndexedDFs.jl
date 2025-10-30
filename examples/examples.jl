import DataFrames
import IndexedDFs
df = DataFrames.DataFrame(id=[100, 200, 300], a=[1, 2, 3], b=[4, 5, 6])
idf = IndexedDFs.IndexedDF(df, "id")
idf[400] = (a=7, b=8)
idf[500] = Dict("a" => 9, "b" => 10)
idf[:, "c"] = [11, 12, 13, 14, 15]
push!(idf, (id=600, a=16, b=17, c=18))
println(idf)
