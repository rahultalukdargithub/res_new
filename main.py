from fastapi import FastAPI, Query
from scraper import scrape_by_location, scrape_by_name
from typing import Optional

app = FastAPI()


@app.get("/")
def welcome():
    return {"message": "Welcome to the Restaurant Scraper API!"}


@app.get("/scrape/location/")
def get_restaurants_by_location(
    city: str = Query(..., example="Kolkata"),
    area: Optional[str] = Query(None, example="park street"),
    limit: int = 10
):
    data = scrape_by_location(city, area, limit)
    return {
        "message": f"{len(data)} results saved for {city}",
        "data": data
    }



@app.get("/scrape/name/")
def get_restaurant_by_name(name: str):
    data = scrape_by_name(name)
    return {"message": f"Details saved for: {name}", "data": data}
