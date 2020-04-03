"""Connect to Database and create visualizations"""
from sqlalchemy import create_engine
import pandas as pd
from bokeh.io import output_file, show
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
from bokeh.palettes import turbo
import concurrent.futures

call = 'mysql+mysqlconnector://mausolorio:ducinALTUM7!@localhost/s&p500'
engine = create_engine(call)
sql_querry = 'SELECT * FROM materials'
data = pd.read_sql(sql_querry, engine)

# Create a ColumnDataSource from df: source
source = ColumnDataSource(data)

# Create a figure with x_axis_type="datetime":
p = figure(x_axis_type='datetime', x_axis_label='Date',
           y_axis_label='US Dollars', tools='pan,wheel_zoom,box_zoom,reset',
           sizing_mode='stretch_both', title='Sector: Materials')

# Make a list of the unique values from the symbol column: company_list
company_list = data.Symbol.unique().tolist()
n = len(company_list)

# For testing only
# company_list = ['AMCR', 'APD', 'AVY', 'ALB', 'BLL', 'CF']
# n = len(company_list)


# Plot date along the x axis and price along the y axis


def draw_plot(name, color):
    p.line(data['date'][data.Symbol == name],
           data['1. open'][data.Symbol == name],
           color=color, legend_label=name)
    p.circle(data['date'][data.Symbol == name],
             data['1. open'][data.Symbol == name],
             color=color, legend_label=name)
    # The location of the legend labels is controlled by the location property
    p.legend.location = "top_left"
    p.legend.click_policy = "hide"
    p.legend.title = 'Ticker'
    p.legend.title_text_font_style = "bold"
    p.legend.title_text_font_size = "15pt"
    return p


# with concurrent.futures.ThreadPoolExecutor() as executor:
#     args = ((name, color) for name, color in zip(company_list, turbo(n)))
#     executor.map(lambda x: draw_plot(*x), args)


for name, color in zip(company_list, turbo(n)):
    draw_plot(name, color)


# Specify the name of the output file and show the result
output_file('materials.html')
show(p)
