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
    region_name: str
    category:str | None=None
    statecode:str |None = None

@app.post('/getdata')
def region_name_api(filters: FilterRequest):
    try:
        filtered = df
        if filters.region_name:
            filtered = filtered.filter(pl.col('region_name') == filters.region_name)

        if filters.statecode:
            filtered = filtered.filter(pl.col("statecode") == filters.statecode)

        if filtered.height == 0:
            return {'data': []}
        return {
            "data":filtered.to_dicts()
        }  
    except Exception as e:
        return {
            "error": str(e)
        }