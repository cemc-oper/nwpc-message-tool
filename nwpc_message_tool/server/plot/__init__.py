from bokeh.embed import file_html, json_item
from bokeh.resources import CDN
from bokeh.plotting import Figure

from .forecast import get_forecast_time_line
from .cycle import get_cycle_time_line


def get_html(plot: Figure):
    return file_html(plot, CDN, "my plot")


def get_json(plot: Figure):
    return json_item(plot)
