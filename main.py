import csv
import requests
from bs4 import BeautifulSoup
import psycopg2

connection = psycopg2.connect(user="postgres",
                                      password="1234",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="MQL5")
cursor = connection.cursor()



def post(name, price, growth, subs, funds, weeks, dd):
    print(name, price, growth, subs, funds, weeks, dd)
    try:


        d=funds.replace("USD","")
        d4=d
        if "K" in d4:
            d1=d.replace("K","")
            d2=d1.replace(".","")
            d3 = d2 + "000"
            d4=d3
        if "M" in d4:
            d1=d.replace("M","")
            d2=d1.replace(".","")
            d3 = d2 + "000000"
            d4=d3

        d5=int(d4.replace(" ", ""))

        #(name, price, growth, subs, funds, weeks, dd)
        price=int(price.replace(" USD per month", ""))
        growth=int(growth.replace("%", "").replace(" ", ""))
        subs=int(subs)
        weeks=int(weeks)
        dd=int(dd.replace("%", ""))

        #print(name, price, growth, subs, d5, weeks, dd)

        #print(name, int(price.replace(" USD per month","")), int(growth.replace("%", "")), int(subs), 100, int(weeks), int(dd.replace("%", "")))

        postgres_insert_query = """ INSERT INTO MqlData ( NAME, Price,Growth,Subs,Funds,Weeks,DD, Date) VALUES (%s,%s,%s,%s,%s,%s,%s,current_timestamp) """
        record_to_insert = (name,price , growth, subs, d5,weeks, dd)
        cursor.execute(postgres_insert_query, record_to_insert)

        connection.commit()
        count = cursor.rowcount
        #print(count, "Record inserted successfully into mobile table")

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into mobile table", error)

    # finally:
    #     if connection:
    #         cursor.close()
    #         connection.close()
    #         print("PostgreSQL connection is closed")

filename = 'signals.csv'

f = csv.writer(open(filename, 'w', newline=''))
f.writerow(['Name', 'Price', 'Growth', 'Monthly Growth', 'Subs', 'Funds', 'Weeks', 'Trades', 'Winrate', 'DD'])

html = requests.get('https://www.mql5.com/en/signals/mt5/list/')
soup = BeautifulSoup(html.text, 'html.parser')

amountOfPages = int(soup.find(class_="paginatorEx").findAll('a')[-1].text)

for pageNumber in range(1, amountOfPages):
    print('## Page: ' + str(pageNumber))
    requestedHtml = requests.get('https://www.mql5.com/en/signals/mt5/list/page' + str(pageNumber))
    pageSoup = BeautifulSoup(requestedHtml.text, 'html.parser')

    page = pageSoup.find(class_="signals-table")
    list = page.findAll(class_="row signal")

    for row in list:
        name = row.find('span', {'class': 'name'}).text
        price = row.find('div', {'class': 'col-price'}).text
        growth = row.find('div', {'class': 'col-growth'}).text
        monthGrowth = row.find('div', {'class': 'col-growth'}).get('title', 'no title')
        subs = row.find('div', {'class': 'col-subscribers'}).text
        funds = row.find('div', {'class': 'col-facilities'}).text
        weeks = row.find('div', {'class': 'col-weeks'}).text
        trades = row.find('div', {'class': 'col-trades'}).text
        winrate = row.find('div', {'class': 'col-plus'}).text
        dd = row.find('div', {'class': 'col-drawdown'}).text

        # print(name, price, growth, monthGrowth, subs, funds, weeks, trades, winrate, dd)
        post(name, price, growth, subs, funds, weeks, dd)
        f.writerow([name, price, growth, monthGrowth, subs, funds, weeks, trades, winrate, dd])


