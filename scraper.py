from serpapi import GoogleSearch
import os
from dotenv import load_dotenv
from zomato_scrapper import scrapper
from zs import scrapper

# Load environment variables from .env
load_dotenv()

API_KEY = os.getenv("SERPAPI_KEY")



def scrape_by_location(city, area, limit=50):
    return scrapper(city, area, limit)


def scrape_by_name(name):
    params = {
        "engine": "google_maps",
        "q": name,
        "type": "search",
        "api_key": API_KEY
    }
    search = GoogleSearch(params)
    results = search.get_dict()
    place = results.get("local_results", [])[0] if results.get("local_results") else {}
    # print(place.get("title", ""), place.get("address", ""), place.get("phone", ""))
    return [[
        place.get("title", ""),
        place.get("address", ""),
        place.get("phone", "")
    ]]


















# def geocode_with_nominatim(location):
#     url = "https://nominatim.openstreetmap.org/search"
#     params = {
#         "q": location,
#         "format": "json",
#         "limit": 1
#     }
#     headers = {
#         "User-Agent": "restaurant-scraper-bot"
#     }

#     response = requests.get(url, params=params, headers=headers).json()
#     if response:
#         lat = response[0]["lat"]
#         lon = response[0]["lon"]
#         print(f"Geocoded {location} to lat: {lat}, lon: {lon}")
#         return float(lat), float(lon)
#     else:
#         return None, None



# def create_excel_if_not_exists(filename):
#     if not os.path.exists(filename):
#         wb = openpyxl.Workbook()
#         ws = wb.active
#         ws.append(["Name", "Address", "Contact"])
#         wb.save(filename)

# def save_to_excel(filename, rows):
#     wb = openpyxl.load_workbook(filename)
#     ws = wb.active
#     for row in rows:
#         ws.append(row)
#     wb.save(filename)




# def scrape_by_location(location_name, limit=50):
#     lat, lng = geocode_with_nominatim(location_name)
#     if not lat or not lng:
#         return [["❌ Could not geocode location: " + location_name]]

#     all_data = []
#     seen = set()
#     start = 0

#     while len(all_data) < limit:
#         params = {
#             "engine": "google_maps",
#             "q": f"Zomato site listed all restaurants cafes {location_name}",
#             "type": "search",
#             "ll": f"@{lat},{lng},10z",             
#             "start": start,
#             "google_domain": "google.co.in",
#             "hl": "en",
#             "gl": "in",                             
#             "location": location_name,             
#             "api_key": API_KEY
#         }

#         try:
#             search = GoogleSearch(params)
#             results = search.get_dict()
#             places = results.get("local_results", [])

#             if not places:
#                 break  # No more results

#             for place in places:
#                 if len(all_data) >= limit:
#                     break

#                 name = place.get("title", "").strip()
#                 address = place.get("address", "").strip()
#                 phone = place.get("phone", "NA").strip()

#                 # Avoid duplicates
#                 unique_key = (name, address)
#                 if unique_key in seen:
#                     continue
#                 seen.add(unique_key)

#                 all_data.append([name, address, phone])

#             start += 20  # pagination step

#         except Exception as e:
#             all_data.append([f"❌ Failed to fetch: {str(e)}"])
#             break

#     return all_data



