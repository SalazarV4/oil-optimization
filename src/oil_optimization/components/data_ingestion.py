import time
from pathlib import Path
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

def scrape_table_rows(web_driver):
    table_head = web_driver.find_element(By.TAG_NAME, 'thead')
    content = table_head.find_elements(By.TAG_NAME, 'th')

    table_body = web_driver.find_element(By.TAG_NAME, 'tbody')
    rows = table_body.find_elements(By.TAG_NAME, 'tr')

    columns = [cont.text for cont in content]
    data = np.array([row.text.split() for row in rows])

    for i in np.arange(2,data.shape[1]):
        data[:,2] = [x.replace(',','.') for x in data[:,2]]

    df = pd.DataFrame(data,columns=columns)
    cols = df.columns[2:]
    df[cols] = df[cols].astype(float)

    return df

def update_csv(file_path, new_df):
    if file_path.exists():
        df_old = pd.read_csv(file_path)
        last_saved_year, last_saved_month = df_old.iloc[0,:2]
        last_new_year, last_new_month = new_df.iloc[0,:2]

        if (last_saved_year == last_new_year) and (last_saved_month == last_new_month):
            print('There are no new prices.')
            return


    new_df.to_csv(file_path,index=False)
    print('Data Updated')

if __name__ == '__main__':
    urls = []
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)

    opts = Options()
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    opts.add_argument("--headless=new")

    for url in urls:
        driver = webdriver.Chrome(options=opts)
        file_name = url.split('/')[-1].split('.')[0]
        f_path = data_dir / f'{file_name}.csv'
        driver.get(url)
        time.sleep(2)

        df_scraped = scrape_table_rows(driver)
        update_csv(f_path, df_scraped)
        driver.quit()
