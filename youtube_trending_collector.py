"""
YouTube Trending Collector

Description:
This script collects the YouTube "mostPopular" (trending) videos for a list
of world regions (ISO 3166-1 alpha-2 codes), enriches the data with human-
readable category names and continents, categorizes video durations into
bins (very short → very long), computes per-region category breakdowns, and
writes the final cleaned dataset to CSV.


Important notes (read before running):
- This script uses the YouTube Data API v3. You need an API key with
sufficient quota. Calls include videos.list (chart=mostPopular) and
videoCategories.list for many regions which may consume a lot of quota.

"""
# --------------------------- IMPORTS & CLIENT -------------------------------
# Set up libraries and the YouTube API client.
from googleapiclient.discovery import build
import time
import pandas as pd
from collections import defaultdict
from googleapiclient.errors import HttpError
import numpy as np

# paste your api key 
api_key = "api-key"
# Connect with youtube api
# NOTE: This creates a googleapiclient discovery client for the YouTube API
# using your provided api_key. Keep your key secret (do not commit it to git).
youtube = build('youtube', 'v3', developerKey=api_key)

# --------------------------- REGION CONFIG ---------------------------------
# WORLD_REGIONS: a long list of ISO 3166-1 alpha-2 country codes. The
# subsequent logic iterates through these to request 'mostPopular' videos
# for each region. 
WORLD_REGIONS = [
    # Africa
    "DZ", "AO", "BJ", "BW", "BF", "BI", "CM", "CV", "CF", "TD", "KM", "CG", "CD",
    "CI", "DJ", "EG", "GQ", "ER", "ET", "GA", "GM", "GH", "GN", "GW", "KE", "LS",
    "LR", "LY", "MG", "MW", "ML", "MR", "MU", "YT", "MA", "MZ", "NA", "NE", "NG",
    "RW", "RE", "SH", "ST", "SN", "SC", "SL", "SO", "ZA", "GS", "SS", "SD", "TN",
    "TG", "TZ", "EH", "ZM", "ZW",

    # Americas - North / Central / Caribbean
    "US", "CA", "MX", "BM", "BB", "BS", "AI", "AG", "BL", "KN", "LC", "MF", "PM",
    "VC", "KY", "TC", "VG", "VI", "PR",

    # Americas - South
    "AR", "BO", "BR", "CL", "CO", "EC", "GF", "GY", "PY", "PE", "SR", "UY", "VE",
    "FK",

    # Asia (including Central & South Asia)
    "AF", "AM", "AZ", "BD", "BT", "BN", "KH", "CN", "HK", "IN", "ID", "IR", "IQ",
    "JP", "KZ", "KP", "KR", "KG", "LA", "LB", "MO", "MY", "MV", "MN", "MM",
    "NP", "PK", "PH", "SG", "LK", "TH", "TL", "VN", "BN", "BD",

    # Middle East (often separated from broader "Asia")
    "SA", "AE", "QA", "KW", "BH", "OM", "YE", "JO", "SY", "PS", "TR", "IQ",
    "IR",

    # Europe
    "AL", "AD", "AT", "BY", "BE", "BA", "BG", "HR", "CY", "CZ", "DK", "EE", "FO",
    "FI", "FR", "GF", "PF", "TF", "DE", "GI", "GR", "GL", "HU", "IS", "IE", "IM",
    "IT", "JE", "LV", "LI", "LT", "LU", "MK", "MT", "MD", "MC", "ME", "NL", "NO",
    "PL", "PT", "RO", "RU", "SM", "RS", "SK", "SI", "ES", "SE", "CH", "UA", "GB",
    "XK",

    # Oceania / Pacific
    "AU", "NZ", "FJ", "WS", "PF", "NC", "PG", "SB", "VU", "TO", "TV", "NR", "FM",
    "MH", "MP", "WF", "TK",

    # Small territories & special codes (useful for APIs)
    "AQ", "IO", "UM", "CX", "CC", "KY", "CK", "GG", "GW", "GU", "HT", "HN", "IC",
    "LI", "LU"
]

# --------------------------- FETCH FUNCTIONS --------------------------------
# Below are functions that call YouTube API endpoints and extract useful fields.

# fetch mostPopular for a region
def fetch_trending_for_region(youtube, region_code, max_results=50, parts='snippet,statistics,contentDetails,topicDetails'):
    # Purpose: Call videos.list(chart='mostPopular') for one region and return
    # a list of simplified dicts (one per video) with only the fields used later.
    try:
        resp = youtube.videos().list(
            part=parts,
            chart='mostPopular',
            regionCode=region_code,
            maxResults=max_results
        ).execute()
    except HttpError as e:
        # On failure (quota error, bad region code, transient network), prints an
        # informative message and return an empty list so the caller can continue.
        print(f"HTTP error for {region_code}: {e}")
        return []
    items = resp.get('items', [])
    results = []
    for it in items:
        # Build a compact representation of each video. Using .get() protects
        # against missing keys in the API response.
        vid = {
            'region': region_code,
            'video_id': it['id'],
            'title': it['snippet'].get('title'),
            'channelTitle': it['snippet'].get('channelTitle'),
            'categoryId': it['snippet'].get('categoryId'),
            'publishedAt': it['snippet'].get('publishedAt'),
            'viewCount': int(it.get('statistics', {}).get('viewCount', 0)),
            'likeCount': int(it.get('statistics', {}).get('likeCount', 0)) if 'likeCount' in it.get('statistics', {}) else None,
            'commentCount': int(it.get('statistics', {}).get('commentCount', 0)) if 'commentCount' in it.get('statistics', {}) else None,
            'duration': it.get('contentDetails', {}).get('duration'),
            'topicIds': it.get('topicDetails', {}).get('topicIds') if it.get('topicDetails') else [],
            'description': it['snippet'].get('description')
        }
        results.append(vid)
    return results

# Aggregate across countries
def collect_trending(youtube, countries, per_country=50, sleep_between_calls=0.1):
    # Purpose: iterate over a list of country codes and collect trending videos
    all_videos = []
    for c in countries:
        # Fetch trending videos for the country 'c' and extend the aggregate list
        rows = fetch_trending_for_region(youtube, c, max_results=per_country)
        all_videos.extend(rows)
        # Small sleep reduces chance of hitting the API too fast. Increase if
        # you experience quota or rate-limit problems.
        time.sleep(sleep_between_calls)  # courteous pause
    df = pd.DataFrame(all_videos)
    return df

def fetch_video_categories_for_region(youtube_client, region_code):
    """
    Returns dict mapping categoryId -> categoryName for a single region.
    Uses part='snippet' and regionCode.
    """
    try:
        resp = youtube_client.videoCategories().list(
            part='snippet',
            regionCode=region_code
        ).execute()
    except HttpError as e:
        print(f"videoCategories.list HTTP error for {region_code}: {e}")
        return {}
    mapping = {}
    for item in resp.get('items', []):
        cid = item.get('id')
        title = item.get('snippet', {}).get('title')
        if cid and title:
            mapping[cid] = title
    return mapping

def build_category_map_for_regions(youtube_client, region_codes, sleep_s=0.1):
    """
    For each region in region_codes, call videoCategories.list and build a
    combined dict categoryId -> categoryName. If multiple regions give different
    names for the same id, keep the first encountered name.
    """
    combined = {}
    for rc in region_codes:
        m = fetch_video_categories_for_region(youtube_client, rc)
        # only add ids that we don't already have
        for k, v in m.items():
            if k not in combined:
                combined[k] = v
        time.sleep(sleep_s)
    return combined

# Merge into DataFrame
def attach_category_names(df, cat_map):
    # Purpose: attach human-readable category names to the DataFrame. Any
    # missing mapping becomes 'Unknown' so downstream code handles it gracefully.
    df2 = df.copy()
    df2['categoryName'] = df2['categoryId'].map(cat_map).fillna('Unknown')
    return df2

# Example aggregation and quick print
def top_categories(df_with_names, top_n=10):
    # count unique videos per category
    agg = df_with_names.groupby('categoryName')['videoId'].nunique().sort_values(ascending=False).head(top_n)
    return agg

# --------------------------- MAIN EXECUTION FLOW ---------------------------
# 1) Collect raw trending video metadata for all regions
df = collect_trending(youtube, WORLD_REGIONS, per_country=50)

# 2) Build category mapping across regions
cat_map = build_category_map_for_regions(youtube, WORLD_REGIONS)

# 3) Attach human-readable category names
df = attach_category_names(df, cat_map)

# 4) Make a working copy for cleaning and transformations
df2 = df.copy()

# 5) Cleaning data from unwanted columns and formatting
df2.drop(columns=['categoryId', 'video_id', 'topicIds'], inplace=True)

# 6) Convert publishedAt to timezone-naive datetime
df2["publishedAt"] = pd.to_datetime(df2["publishedAt"], utc=True).dt.tz_localize(None)

# 7) Duration conversions
# - The original values are the ISO8601 duration strings returned by
# YouTube's API (e.g. 'PT1M30S').
# - The code tries to coerce to Timedelta and then reformat
df2['duration'] = pd.to_timedelta(df2['duration'])
df2['duration'] = pd.to_timedelta(df2['duration'], unit='s') \
                                .apply(lambda x: str(x).split(' ')[-1])
                                
# 8) Sort rows by region to produce a stable ordering for reporting
df2.sort_values(by='region', ascending=False, inplace=True)

# ------------------------ CONTINENT / REGION MAPPINGS ----------------------
# Categrorize region to continent 
df2['continent'] = 'Other'  # default value
df2.loc[df2['region'].isin([
    "DZ", "AO", "BJ", "BW", "BF", "BI", "CM", "CV", "CF", "TD", "KM", "CG", "CD",
    "CI", "DJ", "EG", "GQ", "ER", "ET", "GA", "GM", "GH", "GN", "GW", "KE", "LS",
    "LR", "LY", "MG", "MW", "ML", "MR", "MU", "YT", "MA", "MZ", "NA", "NE", "NG",
    "RW", "RE", "SH", "ST", "SN", "SC", "SL", "SO", "ZA", "GS", "SS", "SD", "TN",
    "TG", "TZ", "EH", "ZM", "ZW"]), 'continent'] = 'Africa'
df2.loc[df2['region'].isin([
    "US", "CA", "MX", "GL", "BM", "BB", "BS", "AI", "AG", "BL", "KN", "LC", "MF", "PM",
    "VC", "KY", "TC", "VG", "VI", "PR", "HT", "DO", "CU", "JM", "TT", "BS"]), 'continent'] = 'North America'
df2.loc[df2['region'].isin([
    "AR", "BO", "BR", "CL", "CO", "EC", "GF", "GY", "PY", "PE", "SR", "UY", "VE",
    "FK"]), 'continent'] = 'South America'
df2.loc[df2['region'].isin([
    "AF", "AM", "AZ", "BD", "BT", "BN", "KH", "CN", "HK", "IN", "ID", "IR", "IQ",
    "JP", "KZ", "KP", "KR", "KG", "LA", "LB", "MO", "MY", "MV", "MN", "MM",
    "NP", "PK", "PH", "SG", "LK", "TH", "TL", "VN", "MV", "MY"]), 'continent'] = 'Asia'
df2.loc[df2['region'].isin([
    "TR", "SA", "AE", "QA", "KW", "BH", "OM", "YE"]), 'continent'] = 'Middle East'
df2.loc[df2['region'].isin([
    "AL", "AD", "AT", "BY", "BE", "BA", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE",
    "GI", "GR", "HU", "IS", "IE", "IT", "LV", "LI", "LT", "LU", "MT", "MD", "MC", "ME", "NL", "NO",
    "PL", "PT", "RO", "RU", "SM", "RS", "SK", "SI", "ES", "SE", "CH", "UA", "GB", "XK",
    "GG", "JE", "IM", "FO", "GI", "GL"]), 'continent'] = 'Europe'
df2.loc[df2['region'].isin([
    "AU", "NZ", "FJ", "PG", "SB", "VU", "NC", "PF", "WS", "TO", "TV", "KI", "MH", "FM",
    "MP", "PW", "NR", "NU", "CK", "NF", "TK", "WF", "GU"]), 'continent'] = 'Oceania'

# --------------------------- REGION NAME MAP --------------------------------
# region_map maps two-letter codes to full country names.
region_map = {
    # Africa
    "DZ": "Algeria",
    "AO": "Angola",
    "BJ": "Benin",
    "BW": "Botswana",
    "BF": "Burkina Faso",
    "BI": "Burundi",
    "CM": "Cameroon",
    "CV": "Cape Verde",
    "CF": "Central African Republic",
    "TD": "Chad",
    "KM": "Comoros",
    "CG": "Congo",
    "CD": "Democratic Republic of the Congo",
    "CI": "Ivory Coast",
    "DJ": "Djibouti",
    "EG": "Egypt",
    "GQ": "Equatorial Guinea",
    "ER": "Eritrea",
    "ET": "Ethiopia",
    "GA": "Gabon",
    "GM": "Gambia",
    "GH": "Ghana",
    "GN": "Guinea",
    "GW": "Guinea-Bissau",
    "KE": "Kenya",
    "LS": "Lesotho",
    "LR": "Liberia",
    "LY": "Libya",
    "MG": "Madagascar",
    "MW": "Malawi",
    "ML": "Mali",
    "MR": "Mauritania",
    "MU": "Mauritius",
    "YT": "Mayotte",
    "MA": "Morocco",
    "MZ": "Mozambique",
    "NA": "Namibia",
    "NE": "Niger",
    "NG": "Nigeria",
    "RW": "Rwanda",
    "RE": "Réunion",
    "SH": "Saint Helena",
    "ST": "Sao Tome and Principe",
    "SN": "Senegal",
    "SC": "Seychelles",
    "SL": "Sierra Leone",
    "SO": "Somalia",
    "ZA": "South Africa",
    "SS": "South Sudan",
    "SD": "Sudan",
    "SZ": "Eswatini",
    "TZ": "Tanzania",
    "TG": "Togo",
    "TN": "Tunisia",
    "UG": "Uganda",
    "EH": "Western Sahara",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",

    # Americas - North / Central / Caribbean
    "US": "United States",
    "CA": "Canada",
    "MX": "Mexico",
    "GL": "Greenland",
    "BM": "Bermuda",
    "BB": "Barbados",
    "BS": "Bahamas",
    "AI": "Anguilla",
    "AG": "Antigua and Barbuda",
    "BL": "Saint Barthélemy",
    "KN": "Saint Kitts and Nevis",
    "LC": "Saint Lucia",
    "MF": "Saint Martin (French part)",
    "PM": "Saint Pierre and Miquelon",
    "VC": "Saint Vincent and the Grenadines",
    "KY": "Cayman Islands",
    "TC": "Turks and Caicos Islands",
    "VG": "British Virgin Islands",
    "VI": "United States Virgin Islands",
    "PR": "Puerto Rico",
    "HT": "Haiti",
    "DO": "Dominican Republic",
    "CU": "Cuba",
    "JM": "Jamaica",
    "TT": "Trinidad and Tobago",
    "BS": "Bahamas",

    # Americas - South
    "AR": "Argentina",
    "BO": "Bolivia",
    "BR": "Brazil",
    "CL": "Chile",
    "CO": "Colombia",
    "EC": "Ecuador",
    "GF": "French Guiana",
    "GY": "Guyana",
    "PY": "Paraguay",
    "PE": "Peru",
    "SR": "Suriname",
    "UY": "Uruguay",
    "VE": "Venezuela",
    "FK": "Falkland Islands",

    # Asia (including Central & South Asia)
    "AF": "Afghanistan",
    "AM": "Armenia",
    "AZ": "Azerbaijan",
    "BD": "Bangladesh",
    "BT": "Bhutan",
    "BN": "Brunei",
    "KH": "Cambodia",
    "CN": "China",
    "HK": "Hong Kong",
    "IN": "India",
    "ID": "Indonesia",
    "IR": "Iran",
    "IQ": "Iraq",
    "JP": "Japan",
    "KZ": "Kazakhstan",
    "KP": "North Korea",
    "KR": "South Korea",
    "KG": "Kyrgyzstan",
    "LA": "Laos",
    "LV": "Latvia",
    "LB": "Lebanon",
    "MO": "Macau",
    "MN": "Mongolia",
    "MM": "Myanmar",
    "NP": "Nepal",
    "PK": "Pakistan",
    "PH": "Philippines",
    "SG": "Singapore",
    "LK": "Sri Lanka",
    "TH": "Thailand",
    "TL": "Timor-Leste",
    "VN": "Vietnam",
    "MV": "Maldives",
    "MY": "Malaysia",
    "NP": "Nepal",

    # Middle East / Arabian Peninsula
    "TR": "Turkey",
    "SA": "Saudi Arabia",
    "AE": "United Arab Emirates",
    "QA": "Qatar",
    "KW": "Kuwait",
    "BH": "Bahrain",
    "OM": "Oman",
    "YE": "Yemen",
    "JO": "Jordan",
    "SY": "Syria",
    "PS": "Palestine",
    "IQ": "Iraq",
    "IR": "Iran",

    # Europe
    "AL": "Albania",
    "AD": "Andorra",
    "AT": "Austria",
    "BY": "Belarus",
    "BE": "Belgium",
    "BA": "Bosnia and Herzegovina",
    "BG": "Bulgaria",
    "HR": "Croatia",
    "CY": "Cyprus",
    "CZ": "Czech Republic",
    "DK": "Denmark",
    "EE": "Estonia",
    "FI": "Finland",
    "FR": "France",
    "DE": "Germany",
    "GI": "Gibraltar",
    "GR": "Greece",
    "HU": "Hungary",
    "IS": "Iceland",
    "IE": "Ireland",
    "IT": "Italy",
    "LV": "Latvia",
    "LI": "Liechtenstein",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "MT": "Malta",
    "MD": "Moldova",
    "MC": "Monaco",
    "ME": "Montenegro",
    "NL": "Netherlands",
    "NO": "Norway",
    "PL": "Poland",
    "PT": "Portugal",
    "RO": "Romania",
    "RU": "Russia",
    "SM": "San Marino",
    "RS": "Serbia",
    "SK": "Slovakia",
    "SI": "Slovenia",
    "ES": "Spain",
    "SE": "Sweden",
    "CH": "Switzerland",
    "UA": "Ukraine",
    "GB": "United Kingdom",
    "XK": "Kosovo",
    "GG": "Guernsey",
    "JE": "Jersey",
    "IM": "Isle of Man",
    "FO": "Faroe Islands",
    "GI": "Gibraltar",
    "GL": "Greenland",

    # Oceania / Pacific
    "AU": "Australia",
    "NZ": "New Zealand",
    "FJ": "Fiji",
    "PG": "Papua New Guinea",
    "SB": "Solomon Islands",
    "VU": "Vanuatu",
    "NC": "New Caledonia",
    "PF": "French Polynesia",
    "WS": "Samoa",
    "TO": "Tonga",
    "TV": "Tuvalu",
    "KI": "Kiribati",
    "MH": "Marshall Islands",
    "FM": "Micronesia",
    "MP": "Northern Mariana Islands",
    "PW": "Palau",
    "NR": "Nauru",
    "NU": "Niue",
    "CK": "Cook Islands",
    "NF": "Norfolk Island",
    "TK": "Tokelau",
    "WF": "Wallis and Futuna",
    "GU": "Guam",

    # Special / small territories & islands
    "AQ": "Antarctica",
    "IO": "British Indian Ocean Territory",
    "UM": "United States Minor Outlying Islands",
    "CX": "Christmas Island",
    "CC": "Cocos (Keeling) Islands",
    "BQ": "Bonaire, Sint Eustatius and Saba",
    "CW": "Curaçao",
    "SX": "Sint Maarten (Dutch part)",
    "BL": "Saint Barthélemy",
    "MF": "Saint Martin (French part)",
    "PM": "Saint Pierre and Miquelon",
    "PN": "Pitcairn",
    "GS": "South Georgia and the South Sandwich Islands",
    "HM": "Heard Island and McDonald Islands",
    "TF": "French Southern Territories",
    "VG": "British Virgin Islands",
    "VI": "U.S. Virgin Islands",
    "EH": "Western Sahara",

    # Other / remaining
    "KP": "North Korea",
    "SY": "Syria",
    "TZ": "Tanzania",
    "RE": "Reunion",
    "WS": "Samoa",
    "PS": "Palestine",
    "CF": "Central African Republic",
    "CG": "Congo (Brazzaville)",
    "CD": "Congo (Kinshasa)",
}

df2['region'] = df2['region'].replace(region_map)



# --------------------------- DURATION BINNING -------------------------------
# If durations are strings, convert first: the script converts to Timedelta and
# creates numeric seconds for binning into categories.
# if durations are strings, convert first:
df2['duration'] = pd.to_timedelta(df2['duration'])

secs = df2['duration'].dt.total_seconds()

bins = [0,         30,       120,      600,      1800,     np.inf]   # seconds
labels = ['very short', 'short', 'medium', 'long', 'very long']

# right=False makes intervals [left, right) so "under 30s" = 0 <= x < 30
df2['duration_category'] = pd.cut(secs, bins=bins, labels=labels, right=False, include_lowest=True)

# optional: handle missing durations (NaT) explicitly
df2['duration_category'] = df2['duration_category'].cat.add_categories(['no duration'])
df2.loc[df2['duration'].isna(), 'duration_category'] = 'no duration'

# --------------------------- PER-REGION AGGREGATION -------------------------
# The next block iterates over each unique region name in df2 and computes
# category counts, percentages and the most frequent category. The result
# is concatenated into final_df for downstream reporting.

results = []

for region in df2['region'].unique():
    # Filter by region
    temp = df2[df2['region'] == region]
    
    # Count categories
    category_counts = temp['categoryName'].value_counts()
    
    # Calculate percentages
    category_percentages = (category_counts / category_counts.sum() * 100).round(2) / 100
    
    # Find max percentage and corresponding category
    max_percentage = category_percentages.max()
    max_category = category_percentages.idxmax()
    
    # Create a temporary DataFrame
    temp_df = pd.DataFrame({
        'region': region,
        'categoryName': category_counts.index,
        'count': category_counts.values,
        'percentage': category_percentages.values,
        'max_percentage': max_percentage,
        'max_category': max_category
    })
    
    results.append(temp_df)

final_df = pd.concat(results, ignore_index=True)
final_df = final_df.sort_values(by=['region', 'count'], ascending=[True, False])

# --------------------------- EXPORT (CSV) ----------------------------------
# Save the cleaned per-video DataFrame to CSV.
df3 = df2.copy()
df4 = final_df.copy()

try:
    df3.to_csv("youtube_full_final.csv", index=False)
    df4.to_csv("youtube_region_category_summary.csv", index=False)
    print("CSV files written: youtube_full_final.csv, youtube_region_category_summary.csv")
except Exception as e:
    print("Excel write failed (install openpyxl to enable):", e)
