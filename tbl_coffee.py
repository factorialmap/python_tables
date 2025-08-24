import polars as pl
import requests
import json
import polars.selectors as cs
from great_tables import GT, loc, style

# step1: define url and get the data
url = "https://raw.githubusercontent.com/rich-iannone/great-tables-mini-workshop/refs/heads/main/data/coffee-sales.json"
response = requests.get(url)
response.raise_for_status()
data_json = response.json()

# step2: create polar df
columns = data_json['columns']
data = {}
for col in columns:
    if col['name'] == 'monthly_sales':
        data['monthly_sales'] = [item['values'] if item else None for item in col['values']]
    else:
        data[col['name']] = col['values']

data_coffee = pl.DataFrame(data)


# step 3: make a table
tbl_coffee = GT(data_coffee)

tbl_coffee

(
    tbl_coffee
    .tab_header(title="Coffee Equipment Sales for 2023")
    .tab_spanner(label ="Revenue", columns = cs.starts_with("revenue"))    
    .tab_spanner(label = "Profit", columns= cs.starts_with("profit"))    
    .cols_label(
        revenue_dollars = "Amount",
        revenue_pct = "Percent",
        profit_dollars = "Amount",
        profit_pct = "Percent",
        monthly_sales = "Monthly Sales"
    )
    .fmt_currency(columns = cs.ends_with("dollars"), use_subunits=False)
    .fmt_percent(columns = cs.ends_with("pct"), decimals=0)
    .tab_style(
        style=style.fill(color = "AliceBlue"),
        locations = loc.body(columns=cs.starts_with("revenue"))       
    )
    .tab_style(
        style = style.fill(color = "papayawhip") ,
        locations = loc.body(columns = cs.starts_with("profit"))
    )
    .tab_style(
        style = style.text(weight="bold"),
        locations = loc.body(rows = pl.col("product") == "Total")
    )
    .fmt_nanoplot(
        columns = "monthly_sales",
        plot_type="bar"
    )
    #.fmt_image(
    #    columns = "icon",
    #    path = "../img/"
    #)
    .sub_missing(
        missing_text=""
    )

)


# sources
# https://youtu.be/rrAGYiXBuWQ?si=ffr_yg4Fg5oxUoV6