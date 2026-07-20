from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def test_repository_uses_app_package_layout():
    assert (ROOT / "app" / "__init__.py").is_file()
    assert (ROOT / "app" / "main.py").is_file()
    assert (ROOT / "app" / "core" / "__init__.py").is_file()
    assert (ROOT / "app" / "core" / "config.py").is_file()
    assert (ROOT / "app" / "core" / "config_loader.py").is_file()
    assert (ROOT / "app" / "core" / "bootstrap.py").is_file()
    assert not (ROOT / "app" / "config.py").exists()
    assert not (ROOT / "app" / "config_loader.py").exists()
    assert not (ROOT / "app" / "http_app.py").exists()
    assert not (ROOT / "vision_orchestrator").exists()


def test_python_sources_do_not_import_legacy_package():
    legacy_package = "vision_" + "orchestrator"
    for root in (ROOT / "app", ROOT / "tests"):
        for path in root.rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            assert f"from {legacy_package}" not in source
            assert f"import {legacy_package}" not in source


def test_app_main_help_is_runnable():
    result = subprocess.run(
        [sys.executable, "-m", "app.main", "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
