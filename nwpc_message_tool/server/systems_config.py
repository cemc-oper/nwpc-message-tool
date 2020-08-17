import numpy as np


systems_config = {
    "grapes_gfs_gmf": {
        "start_hours": [
            {
                "start_hour": "00",
                "forecast_hours": np.append(
                    np.arange(0, 123, 3),
                    np.arange(126, 246, 6),
                )
            },
            {
                "start_hour": "06",
                "forecast_hours": np.arange(0, 123, 3)
            },
            {
                "start_hour": "12",
                "forecast_hours": np.append(
                    np.arange(0, 123, 3),
                    np.arange(126, 246, 6),
                )
            },
            {
                "start_hour": "18",
                "forecast_hours": np.arange(0, 123, 3)
            },
        ]
    },
    "grapes_meso_3km": {
        "start_hours": [
            {
                "start_hour": f"{start_hour:02}",
                "forecast_hours": np.arange(0, 37, 1),
            } for start_hour in np.arange(0, 24, 3)
        ]
    },
    "grapes_tym": {
        "start_hours": [
            {
                "start_hour": f"{start_hour:02}",
                "forecast_hours": np.arange(0, 121, 1),
            } for start_hour in np.arange(0, 24, 6)
        ]
    },
    "grapes_meso_10km": {
        "start_hours": [
            {
                "start_hour": f"{start_hour:02}",
                "forecast_hours": np.arange(0, 85, 1),
            } for start_hour in np.arange(0, 24, 12)
        ]
    }
}