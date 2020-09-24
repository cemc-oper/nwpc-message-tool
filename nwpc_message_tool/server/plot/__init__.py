from bokeh.embed import file_html, json_item
from bokeh.resources import CDN


def get_html(plot):
    return file_html(plot, CDN, "my plot")


def get_json(plot):
    return json_item(plot)
