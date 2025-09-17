import requests
from bs4 import BeautifulSoup
import json
import logging
from datetime import datetime
import re

# --- Configuration ---
# Configure logging to see the scraper's activity
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# List of sources, with primary first and backups after.
# The scraper will try them in this order.
SOURCES = [
    {
        "name": "freejobalert",
        "url": "https://www.freejobalert.com",
        "parser": "parse_freejobalert"
    },
    {
        "name": "sarkarijobfind",
        "url": "https://sarkarijobfind.com",
        "parser": "parse_sarkarijobfind"
    },
    {
        "name": "resultbharat",
        "url": "https://www.resultbharat.com",
        "parser": "parse_resultbharat"
    },
    {
        "name": "add247",
        "url": "https://www.adda247.com/jobs/", # Corrected URL
        "parser": "parse_add247"
    }
]

# HTTP headers to mimic a real browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# --- Parser Functions ---

def parse_freejobalert(content):
    """
    Parses the HTML content from freejobalert.com.
    (This is a placeholder based on a typical structure; selectors may need adjustment).
    """
    soup = BeautifulSoup(content, 'html.parser')
    jobs = []
    job_table = soup.find('table') 
    if not job_table:
        return []
        
    for row in job_table.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) > 2:
            try:
                job_title = cells[0].text.strip()
                apply_link = cells[0].find('a')['href']
                deadline = cells[2].text.strip()
                
                if not job_title or not apply_link: continue

                jobs.append({
                    "id": f"fja_{apply_link}",
                    "title": job_title,
                    "organization": "N/A",
                    "deadline": deadline,
                    "applyLink": requests.compat.urljoin("https://www.freejobalert.com", apply_link)
                })
            except (AttributeError, KeyError, IndexError):
                continue
    return jobs

def parse_sarkarijobfind(content):
    """
    Parses the HTML content from sarkarijobfind.com.
    """
    soup = BeautifulSoup(content, 'html.parser')
    jobs = []
    new_updates_heading = soup.find('h3', string=re.compile(r'New Update', re.IGNORECASE))
    if new_updates_heading and new_updates_heading.find_next_sibling('ul'):
        job_list = new_updates_heading.find_next_sibling('ul')
        for item in job_list.find_all('li'):
            try:
                link = item.find('a')
                if not link: continue
                
                job_title = link.text.strip()
                apply_link = link['href']

                jobs.append({
                    "id": f"sjf_{apply_link}",
                    "title": job_title, "organization": "N/A", "deadline": "N/A",
                    "applyLink": apply_link
                })
            except (AttributeError, KeyError):
                continue
    return jobs

def parse_resultbharat(content):
    """
    Parses the HTML content from resultbharat.com.
    """
    soup = BeautifulSoup(content, 'html.parser')
    jobs = []
    tables = soup.find_all('table')
    for table in tables:
        try:
            headers = [th.text.strip() for th in table.find_all('th')]
            if 'Latest Jobs' in headers:
                job_col_index = headers.index('Latest Jobs')
                for row in table.find_all('tr'):
                    cells = row.find_all('td')
                    if len(cells) > job_col_index:
                        link = cells[job_col_index].find('a')
                        if not link: continue
                        
                        job_title = link.text.strip()
                        apply_link = link['href']

                        jobs.append({
                            "id": f"rb_{apply_link}",
                            "title": job_title, "organization": "N/A", "deadline": "N/A",
                            "applyLink": apply_link
                        })
        except (ValueError, IndexError, AttributeError):
            continue
    return jobs

def parse_add247(content):
    """
    Parses the HTML content from adda247.com/jobs.
    (This is a placeholder; selectors need to be verified against the actual site structure).
    """
    soup = BeautifulSoup(content, 'html.parser')
    jobs = []
    # This is an assumed selector. It might be 'article', 'div.job-post', etc.
    job_listings = soup.find_all('div', class_='job-card') 
    for item in job_listings:
        try:
            link_tag = item.find('a')
            if not link_tag: continue

            job_title = link_tag.text.strip()
            apply_link = link_tag['href']

            if not job_title or not apply_link: continue
            
            jobs.append({
                "id": f"add247_{apply_link}",
                "title": job_title, "organization": "N/A", "deadline": "N/A",
                "applyLink": requests.compat.urljoin("https://www.adda247.com", apply_link)
            })
        except (AttributeError, KeyError):
            continue
    return jobs

# --- Main Scraper Logic ---

def fetch_and_parse(source):
    """
    Fetches content from a URL and passes it to the correct parser.
    """
    parser_func_name = source["parser"]
    parser_func = globals().get(parser_func_name)

    if not parser_func:
        logging.error(f"Parser function '{parser_func_name}' not found for source '{source['name']}'.")
        return []

    try:
        logging.info(f"Attempting to scrape {source['name']} at {source['url']}...")
        response = requests.get(source["url"], headers=HEADERS, timeout=20)
        response.raise_for_status()
        
        jobs = parser_func(response.content)
        logging.info(f"Successfully scraped {len(jobs)} jobs from {source['name']}.")
        return jobs
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch content from {source['name']}: {e}")
        return []
    except Exception as e:
        logging.error(f"An error occurred while parsing {source['name']}: {e}")
        return []

def save_to_json(jobs, source_used):
    """
    Saves the final list of jobs and health data.
    """
    output_data = {
        "lastUpdated": datetime.now().isoformat(),
        "totalJobs": len(jobs),
        "jobListings": jobs
    }
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    logging.info(f"Successfully saved {len(jobs)} jobs to data.json.")

    health_data = {
        "ok": bool(jobs),
        "lastChecked": datetime.now().isoformat(),
        "totalActive": len(jobs),
        "sourceUsed": source_used
    }
    with open('health.json', 'w') as f:
        json.dump(health_data, f, indent=4)


if __name__ == "__main__":
    all_jobs = []
    successful_source = "None"
    
    for source in SOURCES:
        jobs_from_source = fetch_and_parse(source)
        if jobs_from_source:
            logging.info(f"Using data from '{source['name']}' as the source for this run.")
            all_jobs = jobs_from_source
            successful_source = source['name']
            break
        else:
            logging.warning(f"'{source['name']}' failed or returned no data. Trying next source...")
            
    if not all_jobs:
        logging.error("All scraping sources failed. No data was collected.")
    
    save_to_json(all_jobs, successful_source)
