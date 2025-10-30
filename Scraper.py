from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
import re
import csv

driver = webdriver.Chrome()

driver.get("https://fbref.com/en/comps/9/history/Premier-League-Seasons")

link_2425 = driver.find_element(By.LINK_TEXT, "2024-2025")

driver.execute_script("window.scrollBy(0, 600);")
time.sleep(1)
link_2425.click()

driver.execute_script("window.scrollBy(0, 1450);")
time.sleep(2)

hrefs = []

for i in range(20):
    try:
        # Find the <tr> with data-row="i"
        row = driver.find_element(By.CSS_SELECTOR, f'tr[data-row="{i}"]')
        # Get the team link inside the squad column
        link = row.find_element(By.CSS_SELECTOR, 'td a')
        hrefs.append(link.get_attribute("href"))
    except:
        print(f"Row {i} not found or no link")
        continue

# LOOP THROUGH ALL TEAMS
for url in hrefs:
    driver.get(url)
    time.sleep(3)

    # EXTRACT TEAM NAME FROM URL: .../Liverpool-Stats → "Liverpool"
    team_name = url.split("/")[-1].replace("-Stats", "").replace("-", "_")
    team_name = re.sub(r"[^a-zA-Z0-9_]", "", team_name)  # Safe filename


    table1 = driver.find_element(By.ID, "stats_standard_9")
    table2 = driver.find_element(By.ID, "matchlogs_for")

    table1_rows = table1.find_elements(By.CSS_SELECTOR, "tbody tr")

    table2_rows = table2.find_elements(By.CSS_SELECTOR, "tbody tr")
# TABLE 1 → standard_stats
    with open(f"{team_name}_standard.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for row in table1_rows:
                cells = row.find_elements(By.XPATH, ".//th | .//td")[:16]
                row_data = [cell.text.strip() for cell in cells]
                if row_data and row_data[0]:  # Skip empty
                    writer.writerow(row_data)

        # TABLE 2 → matchlogs
    with open(f"{team_name}_matchlogs.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for row in table2_rows:
                cells = row.find_elements(By.XPATH, ".//th | .//td")[:18]
                row_data = [cell.text.strip() for cell in cells]
                if row_data and row_data[0]:
                    writer.writerow(row_data)

    print(f"Saved: {team_name}_standard.csv & {team_name}_matchlogs.csv")
