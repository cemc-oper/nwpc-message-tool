def fix_system_name(system: str) -> str:
    mapping = {
        "grapes_gfs_gmf": "nwpc_grapes_gfs",
        "grapes_meso_10km": "nwpc_grapes_meso_10km",
        "grapes_meso_3km": "nwpc_grapes_meso_3km",
    }
    return mapping.get(system, system)

from . import production
