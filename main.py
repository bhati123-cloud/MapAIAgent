# Google Maps Business Details Scraper with Gemini API
# Description: Extracts business details from Google Maps using Playwright and Gemini API, exports to Excel.

import asyncio
import re
import pandas as pd
from playwright.async_api import async_playwright
import os
import requests
from dotenv import load_dotenv
import json
import threading
import tkinter as tk
from tkinter import ttk, messagebox
# Load environment variables
load_dotenv()
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in environment variables.")

OUTPUT_FILE = 'google_maps_businesses.xlsx'
SEARCH_URL = 'https://www.google.com/maps'
GEMINI_MODEL = 'models/gemini-2.0-flash'
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"

async def gemini_generate(prompt):
    headers = {"Content-Type": "application/json"}
    data = {
        "contents": [{"parts": [{"text": prompt}]}]
    }
    backoff = 2
    for attempt in range(3):  # Retry up to 3 times
        try:
            await asyncio.sleep(1 + attempt * backoff)  # Add delay and exponential backoff
            response = requests.post(GEMINI_API_URL, headers=headers, json=data, timeout=15)  # Reduced timeout to 15 seconds
            response.raise_for_status()
            result = response.json()
            text = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')
            # Extract JSON from response (Gemini may wrap JSON in markdown or extra text)
            json_match = re.search(r'\{.*?\}', text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                try:
                    # Use raw_decode to ignore extra data after the JSON object
                    decoder = json.JSONDecoder()
                    obj, _ = decoder.raw_decode(json_str)
                    return obj
                except Exception as e:
                    print(f"Gemini JSON decode error: {e}")
                    return None
            print(f"Gemini response not valid JSON: {text}")
            return None
        except requests.exceptions.Timeout:
            print(f"Gemini API timeout (attempt {attempt+1}): Read timed out. Retrying...")
            await asyncio.sleep((attempt + 1) * backoff)
            continue
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                print(f"Gemini API 429 error (attempt {attempt+1}): Too Many Requests. Backing off...")
                await asyncio.sleep((attempt + 1) * backoff)
                continue
            else:
                print(f"Gemini API error (attempt {attempt+1}): {e}")
                if attempt == 2:
                    return None
        except Exception as e:
            print(f"Gemini API error (attempt {attempt+1}): {e}")
            if attempt == 2:
                return None
            await asyncio.sleep(1)

def extract_with_gemini(raw_text):
    prompt = f"""
Extract the following business details from the text below. Return a JSON object with these keys: Business Name, Business Type, Address, Phone Number, Email, Website. If a field is missing, use an empty string.

Text:
{raw_text}
"""
    return gemini_generate(prompt)

def clean_field(value):
    if not value:
        return ''
    # Remove non-printable characters, keep valid text
    value = re.sub(r'[\u200B-\u200D\uFEFF]', '', value)
    # Remove excessive whitespace and join lines
    lines = [line.strip() for line in value.split('\n') if line.strip()]
    # Remove duplicates while preserving order
    seen = set()
    cleaned = [line for line in lines if not (line in seen or seen.add(line))]
    return ' '.join(cleaned).strip()

async def safe_text(page, selector):
    try:
        elements = await page.query_selector_all(selector)
        for el in elements:
            text = await el.inner_text()
            if text.strip():
                return text.strip()
    except Exception as e:
        print(f"Error in safe_text for selector '{selector}': {e}")
    return ''

async def scrape_google_maps(query):
    data = []
    unique_keys = set()
    max_cards = 25
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        print('Opening Google Maps...')
        await page.goto(SEARCH_URL)
        await page.wait_for_selector('input#searchboxinput', timeout=15000)
        await page.fill('input#searchboxinput', query)
        await page.click('button#searchbox-searchbutton')
        await page.wait_for_selector('div[role="main"]', timeout=15000)
        print('Waiting for results to load...')
        await asyncio.sleep(2)

        results_selector = '.Nv2PK, div[role="article"], .hfpxzc'
        scrollable_selector = 'div[role="main"] div[aria-label][tabindex="0"]'
        try:
            scrollable = await page.query_selector(scrollable_selector)
        except Exception:
            scrollable = None
        if not scrollable:
            scrollable = page

        seen_card_ids = set()
        no_new_cards_scrolls = 0
        max_no_new_cards_scrolls = 10
        while True:
            if len(data) >= max_cards:
                print(f'Reached the extraction limit of {max_cards} cards.')
                break
            cards = await page.query_selector_all(results_selector)
            new_card_found = False
            for idx, card in enumerate(cards):
                if len(data) >= max_cards:
                    break
                card_id = await card.get_attribute('data-result-index') or str(idx)
                if card_id in seen_card_ids:
                    continue
                seen_card_ids.add(card_id)
                new_card_found = True
                try:
                    await card.click()
                    await page.wait_for_selector('h1, .fontHeadlineLarge, .DUwDvf', timeout=10000)
                    await asyncio.sleep(0.2)
                    all_text = await page.evaluate('document.body.innerText')
                    gemini_data = await extract_with_gemini(all_text)
                    if gemini_data:
                        name = gemini_data.get('Business Name', '')
                        business_type = gemini_data.get('Business Type', '')
                        address = gemini_data.get('Address', '')
                        phone = gemini_data.get('Phone Number', '')
                        email = gemini_data.get('Email', '')
                        website = gemini_data.get('Website', '')
                    else:
                        name = await safe_text(page, 'h1, .fontHeadlineLarge, .DUwDvf, [data-item-id="title"]')
                        business_type = await safe_text(page, '.fontBodyMedium button[jsaction*="pane.rating.category"], .skqShb, span:has-text("Category")')
                        address = await safe_text(page, '[data-item-id="address"], .rogA2c, .Io6YTe.fontBodyMedium, .LrzXr')
                        phone = await safe_text(page, '[data-item-id="phone"], .Io6YTe.fontBodyMedium, .UsdlK')
                        if not phone:
                            phone_matches = re.findall(r'(\+?\d[\d\s\-().]{8,}\d)', all_text)
                            phone = phone_matches[0] if phone_matches else ''
                        website = await safe_text(page, 'a[data-item-id="authority"], a[aria-label*="Website"], .rogA2c a, .Io6YTe a')
                        if not website:
                            website_link = await page.query_selector('a[href^="http"]:not([href*="google.com"])')
                            website = await website_link.get_attribute('href') if website_link else ''
                        email = ''
                        email_link = await page.query_selector('a[href^="mailto:"]')
                        if email_link:
                            email = (await email_link.get_attribute('href')).replace('mailto:', '').strip()
                        if not email:
                            email_matches = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', all_text)
                            email = email_matches[0] if email_matches else ''
                    name = clean_field(name)
                    business_type = clean_field(business_type)
                    address = clean_field(address)
                    phone = clean_field(phone)
                    email = clean_field(email)
                    website = clean_field(website)
                    # If email is missing and website exists, try to find Gmail on website
                    if (not email or not re.search(r'@', email)) and website and website.startswith('http'):
                        try:
                            found_gmail = await find_gmail_on_website(website)
                            if found_gmail:
                                email = found_gmail
                        except Exception as e:
                            print(f'Error finding Gmail on website {website}: {e}')
                    dedup_key = (
                        name.lower(),
                        business_type.lower(),
                        address.lower(),
                        phone.lower(),
                        email.lower(),
                        website.lower()
                    )
                    if dedup_key in unique_keys:
                        continue
                    unique_keys.add(dedup_key)
                    data.append({
                        'Business Name': name,
                        'Business Type': business_type,
                        'Address': address,
                        'Phone Number': phone,
                        'Email': email,
                        'Website': website
                    })
                except Exception as e:
                    print(f'Error extracting business {idx+1}: {e}')
                    continue
            # Aggressively scroll to load more cards
            try:
                await scrollable.focus()
            except Exception:
                pass
            try:
                await page.evaluate(f"let el = document.querySelector('{scrollable_selector}'); if(el) el.scrollTo(0, el.scrollHeight);")
            except Exception:
                await page.mouse.wheel(0, 5000)
            await asyncio.sleep(0.3)
            if not new_card_found:
                no_new_cards_scrolls += 1
            else:
                no_new_cards_scrolls = 0
            if no_new_cards_scrolls >= max_no_new_cards_scrolls:
                print('No more new cards found after scrolling, extraction complete.')
                break
        await browser.close()
    return data

def export_to_excel(data, filename):
    try:
        # If file exists, read existing data
        if os.path.exists(filename):
            existing_df = pd.read_excel(filename)
            new_df = pd.DataFrame(data)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            # Remove duplicates by all columns
            combined_df.drop_duplicates(inplace=True)
        else:
            combined_df = pd.DataFrame(data)
        combined_df.to_excel(filename, index=False)
        print(f'Exported {len(combined_df)} businesses to {filename}')
    except Exception as e:
        print(f'Error exporting to Excel: {e}')

def run_scraper_from_ui(query, status_label, button):
    def task():
        try:
            status_label.config(text='Scraping, please wait...')
            results = asyncio.run(scrape_google_maps(query.get()))
            export_to_excel(results, OUTPUT_FILE)
            status_label.config(text='Done!')
            messagebox.showinfo('Success', f'Scraping complete. Exported to {OUTPUT_FILE}')
        except Exception as e:
            status_label.config(text='Error')
            messagebox.showerror('Error', str(e))
        finally:
            button.config(state=tk.NORMAL)
    threading.Thread(target=task).start()

def launch_ui():
    root = tk.Tk()
    root.title('Google Maps Business Scraper')
    root.geometry('500x200')
    
    frame = ttk.Frame(root, padding=20)
    frame.pack(fill=tk.BOTH, expand=True)
    
    label = ttk.Label(frame, text='Enter your Google Maps search query:')
    label.pack(anchor=tk.W, pady=(0,5))
    
    query_var = tk.StringVar()
    entry = ttk.Entry(frame, textvariable=query_var, width=50)
    entry.pack(fill=tk.X, pady=(0,10))
    entry.focus()
    
    status_label = ttk.Label(frame, text='')
    status_label.pack(anchor=tk.W, pady=(10,0))
    
    def on_start():
        if not query_var.get().strip():
            messagebox.showwarning('Input required', 'Please enter a search query.')
            return
        start_button.config(state=tk.DISABLED)
        run_scraper_from_ui(query_var, status_label, start_button)
    
    start_button = ttk.Button(frame, text='Start Scraping', command=on_start)
    start_button.pack(pady=(5,0))
    
    root.mainloop()

async def find_gmail_on_website(url):
    """
    Visit the website, look for a Gmail address under 'Contact Us' or anywhere on the page.
    Returns the first Gmail address found, or an empty string if none found.
    """
    from playwright.async_api import async_playwright
    import re
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(url, timeout=20000)
            await page.wait_for_load_state('domcontentloaded', timeout=10000)
            # Try to find 'Contact Us' section
            contact_selectors = [
                "text=/contact us/i", "text=/contact/i", "a:has-text('Contact')", "a:has-text('Contact Us')"
            ]
            for selector in contact_selectors:
                try:
                    el = await page.query_selector(selector)
                    if el:
                        section_text = await el.inner_text()
                        match = re.search(r"[\w\.-]+@gmail\.com", section_text, re.I)
                        if match:
                            await browser.close()
                            return match.group(0)
                except Exception:
                    continue
            # If not found, search the whole page text
            body_text = await page.evaluate('document.body.innerText')
            match = re.search(r"[\w\.-]+@gmail\.com", body_text, re.I)
            await browser.close()
            if match:
                return match.group(0)
            return ''
        except Exception as e:
            await browser.close()
            print(f"Error visiting {url}: {e}")
            return ''

if __name__ == '__main__':
    launch_ui()