import polars as pl
from fastapi import FastAPI
df = pl.read_csv('Industry Performance Updated Table - Sheet1.csv')

# print(df.head())

app = FastAPI()

@app.get("/aggregate_category")
def aggregate_category(category: str):
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
