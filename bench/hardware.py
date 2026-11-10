"""Hardware profiling utility for benchmark documentation."""

import os
import platform
import subprocess
import sys
from pathlib import Path


def _get_cpu_model() -> str:
    """Retrieve CPU model name from /proc/cpuinfo if available."""
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text().splitlines():
            if line.startswith("model name"):
                parts = line.split(":", 1)
                if len(parts) > 1:
                    return parts[1].strip()
    return platform.processor() or "Unknown CPU"


def _get_ram_gb() -> str:
    """Retrieve total system memory from /proc/meminfo if available."""
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemTotal:"):
                parts = line.split()
                if len(parts) >= 2 and parts[1].isdigit():
                    kb = int(parts[1])
                    return f"{kb / (1024 * 1024):.1f} GiB"
    return "Unknown RAM"


def _get_git_commit() -> str:
    """Retrieve current git commit hash."""
    try:
        res = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return res.stdout.strip()
    except Exception:
        return "Unknown commit"


def _get_pkg_version(module_name: str) -> str:
    """Retrieve package version without failing if uninstalled."""
    try:
        mod = __import__(module_name)
        return getattr(mod, "__version__", "unknown")
    except ImportError:
        return "not installed"


def _get_disk_type() -> str:
    """Detect backing block device type from /sys/block when running on Linux."""
    sys_block = Path("/sys/block")
    if sys_block.exists():
        devs = sorted(p.name for p in sys_block.iterdir())
        if any(d.startswith("nvme") for d in devs):
            return "NVMe SSD"
        if any(d.startswith("vd") for d in devs):
            return "Virtual block store (virtio_blk)"
        for d in devs:
            if d.startswith(("sd", "xvd", "hd")):
                rot = sys_block / d / "queue" / "rotational"
                if rot.exists():
                    val = rot.read_text(encoding="utf-8", errors="ignore").strip()
                    if val == "0":
                        return "SSD (non-rotational block device)"
                    if val == "1":
                        return "HDD (rotational block device)"
    return "Local block storage"


def get_hardware_info() -> dict[str, str]:
    """Emit hardware and environment specification dictionary."""
    cpu_cores = os.cpu_count() or 1
    return {
        "CPU Model": _get_cpu_model(),
        "CPU Cores": str(cpu_cores),
        "Memory": _get_ram_gb(),
        "Disk Type": _get_disk_type(),
        "Operating System": platform.platform(),
        "Python Version": sys.version.split()[0],
        "DuckDB Version": _get_pkg_version("duckdb"),
        "PyArrow Version": _get_pkg_version("pyarrow"),
        "Git Commit": _get_git_commit(),
    }


def format_hardware_block() -> str:
    """Format hardware profile as formatted text block."""
    lines = ["```", "Hardware & Environment:"]
    for key, value in get_hardware_info().items():
        lines.append(f"  {key}: {value}")
    lines.append("```")
    return "\n".join(lines)


if __name__ == "__main__":
    print(format_hardware_block())
