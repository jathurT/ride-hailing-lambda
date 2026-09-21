"""Configuration validators.

These encode couplings that would otherwise be silent runtime bugs, so they are
tested as behaviour rather than treated as boilerplate.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from fleet.common.config import ProcessingSettings, SimulationSettings


class TestSimulationSettings:
    def test_default_is_288x(self, monkeypatch):
        monkeypatch.setenv("SIM_DAY_SECONDS", "300")
        assert SimulationSettings().speedup == 288.0

    def test_epoch_is_made_timezone_aware(self):
        assert SimulationSettings().epoch_sim.tzinfo is not None


class TestWatermarkValidator:
    def test_default_config_is_accepted(self, monkeypatch):
        monkeypatch.setenv("SIM_DAY_SECONDS", "300")
        monkeypatch.setenv("PROC_WATERMARK_SIM_MINUTES", "30")
        assert ProcessingSettings().watermark_sim_minutes == 30

    def test_rejects_a_watermark_too_tight_for_real_jitter(self, monkeypatch):
        """Speeding the clock up without raising the watermark must not start."""
        monkeypatch.setenv("SIM_DAY_SECONDS", "60")  # 1440x
        monkeypatch.setenv("PROC_WATERMARK_SIM_MINUTES", "30")
        with pytest.raises(ValidationError, match="lateness tolerance"):
            ProcessingSettings()

    def test_error_names_the_variable_to_change(self, monkeypatch):
        monkeypatch.setenv("SIM_DAY_SECONDS", "60")
        monkeypatch.setenv("PROC_WATERMARK_SIM_MINUTES", "30")
        with pytest.raises(ValidationError) as exc:
            ProcessingSettings()
        assert "PROC_WATERMARK_SIM_MINUTES" in str(exc.value)
        assert "plan/03" in str(exc.value)


class TestWindowValidator:
    def test_slide_must_divide_window(self, monkeypatch):
        monkeypatch.setenv("PROC_WINDOW_SIM_MINUTES", "15")
        monkeypatch.setenv("PROC_WINDOW_SLIDE_SIM_MINUTES", "4")
        with pytest.raises(ValidationError, match="multiple"):
            ProcessingSettings()

    def test_default_15_over_5_is_fine(self, monkeypatch):
        monkeypatch.setenv("PROC_WINDOW_SIM_MINUTES", "15")
        monkeypatch.setenv("PROC_WINDOW_SLIDE_SIM_MINUTES", "5")
        assert ProcessingSettings().window_sim_minutes == 15


class TestIdleThresholds:
    def test_critical_must_exceed_warning(self, monkeypatch):
        monkeypatch.setenv("PROC_IDLE_ALERT_SIM_MINUTES", "90")
        monkeypatch.setenv("PROC_IDLE_CRITICAL_SIM_MINUTES", "45")
        with pytest.raises(ValidationError, match="exceed"):
            ProcessingSettings()
