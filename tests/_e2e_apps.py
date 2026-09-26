"""ASGI apps for `test_client.py`, loaded by pyvoy as `module:attr` strings.

Kept in a separate, importable module (rather than built inline in the test)
because pyvoy spawns the ASGI app in its own worker process, which re-imports
the app by name -- it can't just be handed a Python object.
"""

from starlette.applications import Starlette
from starlette.routing import Mount

from grpcreflect.server import mount_reflection_handlers

v1_app, v1alpha_app = mount_reflection_handlers()

app_all = Starlette(routes=[Mount(a.path, app=a) for a in (v1_app, v1alpha_app)])
app_v1alpha_only = Starlette(routes=[Mount(v1alpha_app.path, app=v1alpha_app)])
