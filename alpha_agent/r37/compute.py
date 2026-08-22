"""alpha_agent.r37.compute - the ONE Release-37 compute inventory owner.

READ ONLY. This module measures the workstation and writes down what it found.
It installs nothing, downloads no model weights, buys no cloud time and does not
so much as import a machine-learning framework - it asks the package metadata
whether one is present, which is not the same as loading it.

The measurement matters because Track C's question is not "which model is
fashionable?" but "which model families can this machine actually run on the
data we are about to recommend buying?", and that question has a different
answer on 4 GB of VRAM than on 80 GB.
"""
from __future__ import annotations

import datetime as _dt
import os
import platform
import shutil
import subprocess
import sys
from importlib import metadata as _metadata
from pathlib import Path
from typing import Optional

from .. import r37
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r37.compute"
SCHEMA = "r37_compute_inventory/1"
ARTIFACT_NAME = "compute_inventory.json"

C = _contract

#: Libraries whose PRESENCE decides which model families are reachable today
#: without an install. Presence is read from package metadata; none is imported.
LIBRARY_PROBE = (
    "numpy", "pandas", "scipy", "scikit-learn", "statsmodels",
    "xgboost", "lightgbm", "catboost",
    "torch", "tensorflow", "jax",
    "transformers", "accelerate", "safetensors",
    "pyarrow", "polars", "numba", "onnxruntime",
    "tabpfn", "chronos-forecasting", "gluonts", "darts",
)

#: Paths whose free space constrains what can be stored. The system drive is
#: listed separately because a 500 GB options archive on a 7 GB drive is not a
#: storage question, it is a refusal.
STORAGE_PROBE = ("C:\\", "D:\\")

#: nvidia-smi is queried with a short timeout and never with shell=True. A
#: machine without it records UNMEASURED rather than failing the release.
NVIDIA_SMI_TIMEOUT_SECONDS = 20


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def _total_ram_gb() -> tuple:
    """Installed RAM, and the mechanism that measured it.

    ``os.sysconf`` does not exist on Windows, which is the platform this estate
    runs on, so a sysconf-only implementation reports the machine's memory as
    unknown and every RAM-dependent feasibility verdict silently falls through
    to the VRAM test. The Windows path reads ``GlobalMemoryStatusEx`` through
    ctypes: read-only, standard library, no install.
    """
    try:
        return round(os.sysconf("SC_PAGE_SIZE")
                     * os.sysconf("SC_PHYS_PAGES") / 1e9, 2), "os.sysconf"
    except (AttributeError, ValueError, OSError):
        pass
    if sys.platform == "win32":
        try:
            import ctypes

            class _MemoryStatusEx(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong),
                            ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong),
                            ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong),
                            ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong),
                            ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]

            status = _MemoryStatusEx()
            status.dwLength = ctypes.sizeof(_MemoryStatusEx)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(
                    ctypes.byref(status)):
                return (round(status.ullTotalPhys / 1e9, 2),
                        "kernel32.GlobalMemoryStatusEx")
        except Exception:  # noqa: BLE001 - an unmeasurable machine says so
            pass
    return None, "UNMEASURED_ON_THIS_PLATFORM"


def cpu_and_memory() -> dict:
    """Processor and RAM, from the standard library only."""
    total_ram_gb, source = _total_ram_gb()
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "logical_cpus": os.cpu_count(),
        "python_version": sys.version.split()[0],
        "python_executable": sys.executable,
        "total_ram_gb": total_ram_gb,
        "total_ram_source": source,
    }


def gpu(*, runner=None) -> dict:
    """GPU name, VRAM, driver and compute capability, or an honest UNMEASURED."""
    if runner is not None:
        return runner()
    exe = shutil.which("nvidia-smi")
    if not exe:
        return {"state": "UNMEASURED", "reason": "NVIDIA_SMI_NOT_ON_PATH",
                "devices": []}
    try:
        proc = subprocess.run(  # noqa: S603 - fixed argv, no shell
            [exe, "--query-gpu=name,memory.total,driver_version,compute_cap",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True,
            timeout=NVIDIA_SMI_TIMEOUT_SECONDS, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        return {"state": "UNMEASURED", "reason": type(exc).__name__,
                "devices": []}
    devices = []
    for line in (proc.stdout or "").splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 4:
            continue
        try:
            vram_mib = int(float(parts[1]))
        except ValueError:
            vram_mib = None
        devices.append({"name": parts[0], "vram_mib": vram_mib,
                        "vram_gb": (round(vram_mib / 1024, 2) if vram_mib
                                    else None),
                        "driver_version": parts[2],
                        "compute_capability": parts[3]})
    return {"state": "MEASURED" if devices else "UNMEASURED",
            "returncode": proc.returncode, "devices": devices}


def cuda_toolkit() -> dict:
    """Is a CUDA toolkit installed? Not installing one is a contract term."""
    candidates = [
        Path(r"C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA"),
        Path("/usr/local/cuda"),
    ]
    found = []
    for path in candidates:
        try:
            if path.exists():
                found.append(str(path))
        except OSError:
            continue
    return {"toolkit_paths_present": found,
            "nvcc_on_path": bool(shutil.which("nvcc")),
            "installed": bool(found or shutil.which("nvcc")),
            "may_install_cuda": C.MAY_INSTALL_CUDA}


def libraries() -> dict:
    """Installed numerical / ML packages, by metadata rather than by import."""
    present, absent = {}, []
    for name in LIBRARY_PROBE:
        try:
            present[name] = _metadata.version(name)
        except _metadata.PackageNotFoundError:
            absent.append(name)
        except Exception:  # noqa: BLE001 - a broken dist must not fail the run
            absent.append(name)
    return {"present": dict(sorted(present.items())),
            "absent": sorted(absent),
            "probe_list": list(LIBRARY_PROBE),
            "note": "presence is read from package metadata; nothing is "
                    "imported and nothing is installed"}


def storage(paths=STORAGE_PROBE) -> dict:
    out = {}
    for raw in paths:
        path = Path(raw)
        try:
            usage = shutil.disk_usage(path)
        except OSError as exc:
            out[str(path)] = {"state": "UNMEASURED", "reason": type(exc).__name__}
            continue
        out[str(path)] = {"state": "MEASURED",
                          "total_gb": round(usage.total / 1e9, 1),
                          "free_gb": round(usage.free / 1e9, 1),
                          "used_gb": round(usage.used / 1e9, 1)}
    research = str(r37.research_root())
    return {"volumes": out, "research_root": research,
            "research_root_volume": os.path.splitdrive(research)[0] or None}


def measure(*, gpu_runner=None) -> dict:
    """The whole read-only inventory."""
    return {"measured_at": _now(),
            "cpu_and_memory": cpu_and_memory(),
            "gpu": gpu(runner=gpu_runner),
            "cuda": cuda_toolkit(),
            "libraries": libraries(),
            "storage": storage()}


def constraints(inventory: dict) -> dict:
    """What the measurement RULES OUT, stated as constraints rather than hopes."""
    devices = (inventory.get("gpu") or {}).get("devices") or []
    vram = max((d.get("vram_gb") or 0.0) for d in devices) if devices else 0.0
    ram = (inventory.get("cpu_and_memory") or {}).get("total_ram_gb")
    cpus = (inventory.get("cpu_and_memory") or {}).get("logical_cpus") or 0
    volumes = (inventory.get("storage") or {}).get("volumes") or {}
    free = {k: v.get("free_gb") for k, v in volumes.items()
            if v.get("state") == "MEASURED"}
    largest_free = max(free.values()) if free else None
    system_free = free.get("C:\\")
    caps = (d.get("compute_capability") for d in devices)
    cap_values = [c for c in caps if c]
    #: Turing GTX parts (compute capability 7.5 without tensor cores) run
    #: fp32 only at useful speed; this matters far more for a sequence model
    #: than the VRAM number does.
    return {
        "max_vram_gb": vram or None,
        "total_ram_gb": ram,
        "logical_cpus": cpus,
        "compute_capabilities": cap_values,
        "largest_free_volume_gb": largest_free,
        "system_volume_free_gb": system_free,
        "gpu_present": bool(devices),
        "gpu_usable_for_small_models": bool(vram and vram >= 3.5),
        "gpu_usable_for_foundation_model_inference": bool(vram and vram >= 8.0),
        "gpu_usable_for_foundation_model_finetuning": bool(vram and vram >= 24.0),
        "cpu_is_the_practical_trainer": bool(cpus and cpus <= 8),
        "storage_supports_a_daily_bar_archive": bool(
            largest_free and largest_free >= 50),
        "storage_supports_an_options_tick_archive": bool(
            largest_free and largest_free >= 600),
        "binding_constraints": sorted(filter(None, [
            "VRAM" if (vram and vram < 8.0) else None,
            "CPU_CORE_COUNT" if (cpus and cpus <= 8) else None,
            "SYSTEM_VOLUME_FREE_SPACE" if (system_free is not None
                                           and system_free < 20) else None,
            "NO_GPU" if not devices else None,
        ])),
    }


def artifact(inventory: dict, *, campaign_id: str, created_at: str) -> dict:
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "inventory": inventory,
        "constraints": constraints(inventory),
        "read_only": True,
        "installed_anything": False,
        "downloaded_model_weights": False,
        "purchased_cloud_compute": False,
        "may_install_cuda": C.MAY_INSTALL_CUDA,
        "may_download_model_weights": C.MAY_DOWNLOAD_MODEL_WEIGHTS,
        "may_purchase_cloud_compute": C.MAY_PURCHASE_CLOUD_COMPUTE,
    }
    return r37.artifact_body(SCHEMA, payload)


def path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r37.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    return r37.write_json(path_for(body["campaign_id"]), body)


def load(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    return r37.read_json(path_for(campaign_id))


__all__ = ["CALCULATION_OWNER", "LIBRARY_PROBE", "STORAGE_PROBE",
           "cpu_and_memory", "gpu", "cuda_toolkit", "libraries", "storage",
           "_total_ram_gb",
           "measure", "constraints", "artifact", "freeze", "load", "path_for"]
