from flask import Blueprint, jsonify, make_response, request

import pandas as pd


main_app = Blueprint('main_app', __name__, template_folder='template')


@main_app.route('/')
def get_index_page():
    return jsonify({
        'status': 'ok'
    })


@main_app.route('/plot/cycle/time-line')
def get_cycle_time_line_plot():
    from nwpc_message_tool.server.plot import get_cycle_time_line

    system = request.args.get("system", None)
    start_time = request.args.get("start-time", None)
    start_time = pd.to_datetime(start_time, format="%Y%m%d%H")
    print(start_time)

    output_html = get_cycle_time_line(
        system=system,
        start_time=start_time
    )
    return make_response(output_html)
