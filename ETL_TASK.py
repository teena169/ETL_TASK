#!pip install aiohttp beautifulsoup4   #installing required libraries to scrape the data
#!pip install requests beautifulsoup4 pandas openpyxl


import requests
from bs4 import BeautifulSoup
import pandas as pd   # have to use pandas as could not import aiohttp
import os
import zipfile

# Base URL of the page to scrape
base_url = 'https://www.scrapethissite.com'

# Directory to save the HTML files
html_dir = 'html_files'

# Function to fetch and parse a single page
def fetch_page(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    return response.text, soup

# Function to extract team stats from a single page
def extract_team_stats(soup):
    table = soup.find('table', {'class': 'table'})
    if table:
        dfs = pd.read_html(str(table))
        return dfs[0]
    else:
        return None

# Function to get all subpage links from the main page
def get_all_page_links(soup):
    pagination = soup.find('ul', {'class': 'pagination'})
    links = pagination.find_all('a', href=True)
    page_links = [base_url + link['href'] for link in links if 'page_num' in link['href']]
    return page_links

# Create directory for HTML files if it doesn't exist
if not os.path.exists(html_dir):
    os.makedirs(html_dir)

# Fetch the main page
main_html, main_soup = fetch_page(base_url + '/pages/forms/')

# Save the main page HTML
with open(os.path.join(html_dir, '1.html'), 'w', encoding='utf-8') as file:
    file.write(main_html)

# Get all subpage links
all_page_links = get_all_page_links(main_soup)

# Ensure unique page links
all_page_links = list(set(all_page_links))

print("Subpage links fetched:")
for link in all_page_links:
    print(link)

# Initialize a list to hold all DataFrames
all_dfs = []
html_files = ['1.html']

# Define the expected columns
expected_columns = ['Team Name', 'Year', 'Wins', 'Losses', 'OT Losses', 'Win %', 'Goals For (GF)', 'Goals Against (GA)', '+ / -']

# Fetch and extract data from the main page and all subpages
for idx, page_link in enumerate(all_page_links, start=2):  # Start numbering from 2 for subpages
    print(f"Fetching page: {page_link}")
    page_html, page_soup = fetch_page(page_link)
    
    # Save each subpage HTML
    html_filename = f'{idx}.html'
    with open(os.path.join(html_dir, html_filename), 'w', encoding='utf-8') as file:
        file.write(page_html)
    html_files.append(html_filename)
    
    df = extract_team_stats(page_soup)
    if df is not None:
        # Ensure the DataFrame has the expected columns
        df.columns = [col.strip() for col in df.columns]  # Strip any leading/trailing whitespace from column names
        missing_columns = set(expected_columns) - set(df.columns)
        if not missing_columns:
            df = df[expected_columns]
            all_dfs.append(df)
        else:
            print(f"Missing columns in page {page_link}: {missing_columns}")
    else:
        print(f"No table found on page: {page_link}")

# Concatenate all DataFrames into a single DataFrame
if all_dfs:
    all_data = pd.concat(all_dfs, ignore_index=True)
else:
    all_data = pd.DataFrame()

# Create a ZipFile containing all HTML files
with zipfile.ZipFile('hockey_team_stats_html.zip', 'w') as z:
    for html_file in html_files:
        z.write(os.path.join(html_dir, html_file), html_file)

# Process and Save Data to Excel
if not all_data.empty:
    required_columns = {'Year', 'Team Name', 'Wins'}
    if required_columns.issubset(all_data.columns):
        all_data['Year'] = all_data['Year'].astype(int)
        summary = all_data.groupby('Year').agg(
            Winner=pd.NamedAgg(column='Team Name', aggfunc=lambda x: x[all_data.loc[x.index, 'Wins'].idxmax()]),
            Winner_Num_of_Wins=pd.NamedAgg(column='Wins', aggfunc='max'),
            Loser=pd.NamedAgg(column='Team Name', aggfunc=lambda x: x[all_data.loc[x.index, 'Wins'].idxmin()]),
            Loser_Num_of_Wins=pd.NamedAgg(column='Wins', aggfunc='min')
        ).reset_index()

        # Save the data to an Excel file with two sheets
        with pd.ExcelWriter('hockey_team_stats.xlsx', engine='openpyxl') as writer:
            all_data.to_excel(writer, index=False, sheet_name='NHL Stats 1990-2011')
            summary.to_excel(writer, index=False, sheet_name='Winner and Loser per Year')

        # Display the DataFrame
        print(all_data)
    else:
        print(f"Missing required columns: {required_columns - set(all_data.columns)}")
else:
    print("No data extracted from the pages.")

