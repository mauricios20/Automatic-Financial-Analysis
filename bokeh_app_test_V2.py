"""Connect to Database and create visualizations"""
from sqlalchemy import create_engine
import pandas as pd
from functools import lru_cache
from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource
from bokeh.models import Select, HoverTool, DateRangeSlider, BoxAnnotation
from bokeh.plotting import figure
from bokeh.palettes import turbo


call = 'mysql+mysqlconnector://mausolorio:ducinALTUM7!@localhost/s&p500'
engine = create_engine(call)
sql_querry = 'SELECT * FROM materials'
data = pd.read_sql(sql_querry, engine)
data.rename(columns={"1. open": "open", "2. high": "high",
                     "3. low": "low", "4. close": "close",
                     "5. volume": "volume"}, inplace=True)

# # Default strat and end date
startdate = data.date.min()
enddate = data.date.max()
# Make a list of the unique values from the symbol column: company_list
# company_list = data.Symbol.unique().tolist()
# n = len(company_list)

# Create a ColumnDataSource from df: source
company_list = ['AMCR', 'APD', 'AVY', 'ALB', 'BLL', 'CF']
n = len(company_list)


@lru_cache()
def get_data(company):
    dtf = data[data.Symbol == company].set_index('date')
    for name, color in zip(company_list, turbo(n)):
        if name == company:
            dtf['color'] = color
    return dtf


# set up widgets widgets
ticker = Select(value='AMCR', options=company_list, title="Company")
range_slider = DateRangeSlider(start=startdate, end=enddate,
                               value=(startdate, enddate), step=1,
                               title='Date Range')
# Box selection
box = BoxAnnotation(fill_alpha=0.5, line_alpha=0.5,
                    level='underlay', left=startdate, right=enddate)

# set up plots Main Figure
source = ColumnDataSource(data=dict(date=[], open=[],
                                    high=[], low=[], close=[],
                                    volume=[], Symbol=[], color=[]))

p = figure(x_axis_type='datetime', x_axis_label='Date',
           y_axis_label='US Dollars', tools='pan,wheel_zoom,box_zoom,reset',
           sizing_mode='scale_width', title='Sector: Materials',
           x_range=(startdate, enddate))

p.line('date', 'open', color='black', legend_field='Symbol', source=source)
p.circle('date', 'open', color='color', legend_field='Symbol', source=source)

p.legend.location = "top_left"
p.legend.click_policy = "hide"
p.legend.title = 'Ticker'
p.legend.title_text_font_style = "bold"
p.legend.title_text_font_size = "15pt"
# Create a HoverTool: hover
hover = HoverTool(tooltips=[('Price', '@open')])

# Add the HoverTool to the plot
p.add_tools(hover)

# Second FIgure with Volumes
p2 = figure(plot_height=100, sizing_mode='scale_width',
            x_axis_type='datetime', toolbar_location=None, active_drag=None)
p2.vbar(x='date', top='volume', color='color', source=source)
p2.add_layout(box)

# set up callbacks


def ticker_change(attrname, old, new):
    ticker.options = company_list
    update()


def update_range(attr, old, new):
    box.left = new[0]
    box.right = new[1]
    p.x_range.start = new[0]
    p.x_range.end = new[1]


range_slider.on_change('value', update_range)


def update(selected=None):
    t1 = ticker.value
    df = get_data(t1)
    range_slider.start = df.index.min()
    range_slider.end = df.index.max()
    range_slider.value = (df.index.min(), df.index.max())
    data = df[['open', 'high', 'low', 'close', 'volume', 'Symbol', 'color']]
    source.data = data


ticker.on_change('value', ticker_change)


def selection_change(attrname, old, new):
    t1 = ticker.value
    data = get_data(t1)
    selected = source.selected.indices


source.selected.on_change('indices', selection_change)

# set up layout
widgets = row(ticker)
layout = column(widgets, p, p2, range_slider, sizing_mode="stretch_width")

# initialize
update()

curdoc().add_root(layout)
curdoc().title = "Stocks"

# bokeh serve --show C:\Users\mauri\
# github\Automatic-Financial-Analysis\bokeh_app_test_V2.py
