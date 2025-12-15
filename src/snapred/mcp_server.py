import inspect
from typing import Any, Dict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from snapred.backend.service.ApiService import ApiService
from snapred.backend.service.ServiceDirectory import ServiceDirectory
from snapred.backend.service.ServiceFactory import ServiceFactory

app = FastAPI(title="SNAPRed MCP")

service_dir = ServiceDirectory()
# Instantiate ServiceFactory to auto-register available services in the ServiceDirectory
ServiceFactory()
api_service = ApiService()


@app.get("/mcp")
def list_paths():
    """Return the available services and their paths."""
    return api_service.getValidPaths()


@app.get("/mcp/{service}/{subpath}/parameters")
def get_parameters(service: str, subpath: str):
    """Return the parameter schema (if any) for a specific service path."""
    path = f"{service}/{subpath}" if subpath else service
    try:
        return api_service.getPathParameters(path)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/mcp/{service}/{subpath}")
def call_service(service: str, subpath: str, payload: Dict[str, Any] | None = None):
    """Invoke a registered service path with the given JSON payload.

    The endpoint will attempt to coerce the payload into the function's annotated
    pydantic model if one is declared.
    """
    # path = f"{service}/{subpath}" if subpath else service

    if service not in service_dir:
        raise HTTPException(status_code=404, detail=f"Service not found: {service}")

    svc = service_dir[service]
    paths = svc.getPaths()
    if subpath not in paths:
        raise HTTPException(status_code=404, detail=f"Path not found: {subpath} on service {service}")

    func = paths[subpath]
    try:
        sig = inspect.signature(func)
        if len(sig.parameters) == 0:
            result = func()
        else:
            # single-argument routes are common: try to coerce payload when possible
            param = next(iter(sig.parameters.values())).annotation
            arg = payload
            if inspect.isclass(param) and issubclass(param, BaseModel):
                # If payload is None, let the model raise an informative error
                if payload is None:
                    arg = param()
                elif isinstance(payload, dict):
                    arg = param(**payload)
                else:
                    arg = param.parse_obj(payload)
            result = func(arg)
        return result
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e))
