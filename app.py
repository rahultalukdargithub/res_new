import streamlit as st
import pandas as pd
import requests
from io import BytesIO
import openpyxl
from scraper import scrape_by_location, scrape_by_name

st.set_page_config(page_title="Restaurant Scraper", layout="centered")
st.title("🍽️ Restaurant Scraper Assistant")


def scrapper(city , area , no_of_restaurants):
    city = city.strip().lower().replace(" ", "-")
    area = area.strip().lower().replace(" ", "-") if area else None
    base_url = "https://concurrentzomatofetch-production.up.railway.app"
    if(area):
        j = requests.get(base_url + f"/api/data/location?city={city}&area={area}&limit={no_of_restaurants}")
    else:
        j =requests.get(base_url + f"/api/data/location?city={city}&limit={no_of_restaurants}")

    json_data = j.json()
    data = []
    for it in json_data['details']:
        data.append([
            it['name'],
            it['address'],
            it['phone']
        ])
    st.success(json_data.get("message", "Scraped successfully."))
    return data

# Step 0: Session state for accumulated results
if "results_df" not in st.session_state:
    st.session_state.results_df = pd.DataFrame(columns=["Name", "Address", "Phone"])

if "history" not in st.session_state:
    st.session_state.history = []


def reset_results_df():
    st.session_state.results_df = pd.DataFrame(columns=["Name", "Address", "Phone"])

# File mode toggle with reset on change
file_mode = st.radio(
    "Do you already have an Excel file?",
    ["❌ No, I don't have one", "✅ Yes, I want to upload and update"],
    key="file_mode",
    on_change=reset_results_df
)

if file_mode == "✅ Yes, I want to upload and update":
    uploaded_file = st.file_uploader("📁 Upload your Excel file:", type=["xlsx"])
    if uploaded_file:
        try:
            uploaded_df = pd.read_excel(uploaded_file)
            st.session_state.results_df = pd.concat(
                [uploaded_df, st.session_state.results_df],
                ignore_index=True
            )
            st.success("✅ Excel file loaded and merged successfully!")
            
        except Exception as e:
            st.error(f"Error reading file: {e}")
            st.stop()
    else:
        st.stop()

# Step 2: Select scrape method
scrape_type = st.selectbox("Choose what you want to search by:", ["Location", "Restaurant Name"])

if scrape_type == "Location":
    City = st.text_input("📍 Enter City (e.g., Kolkata):")
    area = st.text_input("📍 Enter Area (e.g., Park Street)(Optional):")
    limit = st.slider("🔢 Number of restaurants", 1, 2000, 10)

    if st.button("🔍 Scrape by Location"):
        if not City:
            st.warning("Please enter the City.")
        else:
            with st.spinner("Fetching restaurants..."):
                try:
                    # response = requests.get(
                    #     "https://serpapi-restaurants.onrender.com/scrape/location/",
                    #     params={"city": City,"area":area, "limit": limit}
                    # )
                    # result = response.json()
                    # data = result.get("data", [])
                    data = scrape_by_location(City, area, limit)
                    new_df = pd.DataFrame(data, columns=["Name", "Address", "Phone"] )
                    new_df.index+=1
                    new_df = new_df.dropna()
                    
                    st.dataframe(new_df)
                    st.session_state.results_df = pd.concat(
                        [st.session_state.results_df, new_df],
                        ignore_index=True
                    )
                    st.session_state.history.append({
                        "type": "📍 Location",
                        "query": f"{City} , {area}",
                        "count": len(new_df)
                    })

                except Exception as e:
                    st.error(f"Error during scraping: {e}")

elif scrape_type == "Restaurant Name":
    name = st.text_input("🍽️ Enter restaurant name (e.g., Arsalan Restaurant):")

    if st.button("🔍 Scrape by Restaurant Name"):
        if not name:
            st.warning("Please enter a restaurant name.")
        else:
            with st.spinner("Fetching restaurant details..."):
                try:
                    response = requests.get(
                        "https://serpapi-restaurants.onrender.com/scrape/name/",
                        params={"name": name}
                    )
                    result = response.json()
                    data = result.get("data", [])
                    new_df = pd.DataFrame(data, columns=["Name", "Address", "Phone"])
                    st.success(result.get("message", "Scraped successfully."))
                    st.dataframe(new_df)
                    st.session_state.results_df = pd.concat(
                        [st.session_state.results_df, new_df],
                        ignore_index=True
                    )
                    st.session_state.history.append({
                        "type": "🍽️ Restaurant",
                        "query": name,
                        "count": len(new_df)
                    })

                except Exception as e:
                    st.error(f"Error during scraping: {e}")

# Step 4: Download Combined Results
if not st.session_state.results_df.empty:
    st.markdown("### 📥 Download Updated Excel File")
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        st.session_state.results_df.to_excel(writer, index=False)
    st.download_button(
        label="⬇️ Download Excel",
        data=output.getvalue(),
        file_name="restaurants_updated.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
# Step 5: Clear session state
st.markdown("### 🧹 Clear Results")

if st.button("🗑️ Reset All"):
    st.session_state.results_df = pd.DataFrame(columns=["Name", "Address", "Phone"])
    st.session_state.history = []
    st.success("✅ All data and history cleared. Reloading...")
    

# 📚 Sidebar: Search History
with st.sidebar:
    st.markdown("## 🔎 Search History")
    if st.session_state.history:
        for entry in reversed(st.session_state.history):
            st.markdown(f"- {entry['type']} **{entry['query']}** ({entry['count']} results)")
    else:
        st.info("No searches yet.")


