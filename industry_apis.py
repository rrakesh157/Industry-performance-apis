import polars as pl
from fastapi import FastAPI
from pydantic import BaseModel
df = pl.read_csv('Industry Performance Updated Table - Sheet1.csv')

# print(df.head())

app = FastAPI()

@app.get("/aggregate_category/{category}")
def aggregate_category(category: str | None = None):
    try:
        filtered = df.filter(pl.col("category") == category)

        if filtered.height == 0:
            return {"data": []}

        agg = (
            filtered.group_by("comname")
            .agg([
                pl.sum("total").alias("total_sum"),
                pl.sum("netweight_tmt").alias("netweight_sum")
            ])
            .sort("netweight_sum", descending=True)
        )

        return {"data": agg.to_dicts()}

    except Exception as e:
        return {"error": str(e)}
    


class FilterRequest(BaseModel):
    region: str | None = None
    category: str | None = None
    distname: str | None = None
    group_by: list[str] = ["comname"]  
    agg_columns: list[str] = ["netweight_tmt", "total"] 


@app.post("/filter_and_aggregate")
def filter_and_aggregate(filters: FilterRequest):
    try:
        filtered = df

        if filters.region:
            filtered = filtered.filter(pl.col("region_name") == filters.region)

        if filters.category:
            filtered = filtered.filter(pl.col("category") == filters.category)

        if filters.distname:
            filtered = filtered.filter(pl.col("distname") == filters.distname)

        if filtered.height == 0:
            return {"data": [], "message": "No matching records found"}

        # Build aggregation expressions
        agg_exprs = [pl.sum(col).alias(f"{col}_sum") for col in filters.agg_columns]

        # Perform groupby + aggregation
        result = (
            filtered.group_by(filters.group_by)
            .agg(agg_exprs)
            .sort(filters.group_by)
        )

        return {"data": result.to_dicts()}

    except Exception as e:
        return {"error": str(e)}
    
