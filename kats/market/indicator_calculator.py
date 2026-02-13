"""
KATS IndicatorCalculator - Technical Indicator Computation Engine

All methods are static / pure functions operating on price/volume arrays.
No I/O, no side effects -- suitable for both live trading and back-testing.

Supported indicators:
  - SMA, EMA
  - RSI (Wilder smoothing)
  - VWAP
  - Bollinger Bands
  - ATR (Average True Range)
  - MACD
  - Volume Ratio
  - calculate_all (batch computation from daily OHLCV dicts)
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger(__name__)


class IndicatorCalculator:
    """
    Collection of static methods for technical indicator calculation.

    All methods accept plain Python lists so they stay dependency-free
    (no mandatory numpy/pandas at runtime, though the project does have
    them available for heavier analytics elsewhere).
    """

    # ── Moving Averages ──────────────────────────────────────────────────

    @staticmethod
    def sma(prices: List[float], period: int) -> float:
        """
        Simple Moving Average over the last *period* prices.

        Raises
        ------
        ValueError
            If there are fewer data points than *period*.
        """
        if len(prices) < period:
            raise ValueError(
                f"SMA requires at least {period} prices, got {len(prices)}"
            )
        return sum(prices[-period:]) / period

    @staticmethod
    def ema(prices: List[float], period: int) -> float:
        """
        Exponential Moving Average.

        Uses the standard multiplier ``2 / (period + 1)`` and seeds with
        the SMA of the first *period* values.

        Raises
        ------
        ValueError
            If there are fewer data points than *period*.
        """
        if len(prices) < period:
            raise ValueError(
                f"EMA requires at least {period} prices, got {len(prices)}"
            )
        multiplier = 2.0 / (period + 1)
        # Seed with SMA of the first `period` values
        ema_value = sum(prices[:period]) / period
        for price in prices[period:]:
            ema_value = (price - ema_value) * multiplier + ema_value
        return ema_value

    @staticmethod
    def ema_series(prices: List[float], period: int) -> List[float]:
        """
        Return the full EMA series (same length as input, NaN-padded for
        the first *period - 1* elements).
        """
        if len(prices) < period:
            raise ValueError(
                f"EMA series requires at least {period} prices, got {len(prices)}"
            )
        multiplier = 2.0 / (period + 1)
        result: List[float] = [float("nan")] * (period - 1)
        ema_value = sum(prices[:period]) / period
        result.append(ema_value)
        for price in prices[period:]:
            ema_value = (price - ema_value) * multiplier + ema_value
            result.append(ema_value)
        return result

    # ── RSI (Wilder Smoothing) ───────────────────────────────────────────

    @staticmethod
    def rsi(prices: List[float], period: int = 14) -> float:
        """
        Relative Strength Index using Wilder's smoothing method.

        The first RSI value is the simple average of gains/losses over
        *period*.  Subsequent values use exponential (Wilder) smoothing:
            avg_gain = (prev_avg_gain * (period - 1) + current_gain) / period

        Returns
        -------
        float
            RSI in range [0, 100].
        """
        required = period + 1
        if len(prices) < required:
            raise ValueError(
                f"RSI requires at least {required} prices, got {len(prices)}"
            )

        deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]

        # Initial averages (simple)
        gains = [d if d > 0 else 0.0 for d in deltas[:period]]
        losses = [-d if d < 0 else 0.0 for d in deltas[:period]]
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period

        # Wilder smoothing for remaining deltas
        for delta in deltas[period:]:
            gain = delta if delta > 0 else 0.0
            loss = -delta if delta < 0 else 0.0
            avg_gain = (avg_gain * (period - 1) + gain) / period
            avg_loss = (avg_loss * (period - 1) + loss) / period

        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    # ── VWAP ─────────────────────────────────────────────────────────────

    @staticmethod
    def vwap(
        prices: List[float],
        volumes: List[int],
        highs: List[float],
        lows: List[float],
    ) -> float:
        """
        Volume-Weighted Average Price.

        Typical price = (High + Low + Close) / 3
        VWAP = cumulative(TP * Volume) / cumulative(Volume)

        All four input lists must be the same length.
        """
        if not prices:
            return 0.0
        n = len(prices)
        if len(volumes) != n or len(highs) != n or len(lows) != n:
            raise ValueError("All input lists must have the same length")

        cumul_tp_vol = 0.0
        cumul_vol = 0
        for h, l, c, v in zip(highs, lows, prices, volumes):
            tp = (h + l + c) / 3.0
            cumul_tp_vol += tp * v
            cumul_vol += v

        return cumul_tp_vol / cumul_vol if cumul_vol > 0 else 0.0

    # ── Bollinger Bands ──────────────────────────────────────────────────

    @staticmethod
    def bollinger_bands(
        prices: List[float],
        period: int = 20,
        num_std: float = 2.0,
    ) -> Dict[str, float]:
        """
        Bollinger Bands (SMA +- num_std * standard deviation).

        Returns
        -------
        dict
            {"upper": float, "middle": float, "lower": float}
        """
        if len(prices) < period:
            raise ValueError(
                f"Bollinger Bands require at least {period} prices, got {len(prices)}"
            )
        window = prices[-period:]
        middle = sum(window) / period
        variance = sum((p - middle) ** 2 for p in window) / period
        std = math.sqrt(variance)
        return {
            "upper": middle + num_std * std,
            "middle": middle,
            "lower": middle - num_std * std,
        }

    # ── ATR (Average True Range) ─────────────────────────────────────────

    @staticmethod
    def atr(
        highs: List[float],
        lows: List[float],
        closes: List[float],
        period: int = 14,
    ) -> float:
        """
        Average True Range over *period*.

        True Range = max(H-L, |H - prev_C|, |L - prev_C|)
        ATR = SMA of the last *period* true ranges.

        Requires at least *period + 1* data points (need one previous close).
        """
        required = period + 1
        if len(highs) < required or len(lows) < required or len(closes) < required:
            raise ValueError(
                f"ATR requires at least {required} data points, "
                f"got H={len(highs)} L={len(lows)} C={len(closes)}"
            )

        true_ranges: List[float] = []
        for i in range(1, len(highs)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            true_ranges.append(tr)

        # Wilder smoothing for ATR
        atr_value = sum(true_ranges[:period]) / period
        for tr in true_ranges[period:]:
            atr_value = (atr_value * (period - 1) + tr) / period

        return atr_value

    # ── MACD ─────────────────────────────────────────────────────────────

    @staticmethod
    def macd(
        prices: List[float],
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
    ) -> Dict[str, Optional[float]]:
        """
        Moving Average Convergence Divergence.

        Returns
        -------
        dict
            {"macd": float, "signal": float | None, "histogram": float | None}

        If there are not enough data points for the signal line the signal
        and histogram will be ``None``.
        """
        if len(prices) < slow:
            raise ValueError(
                f"MACD requires at least {slow} prices, got {len(prices)}"
            )

        # Compute full EMA series for fast and slow
        ema_fast_series = IndicatorCalculator.ema_series(prices, fast)
        ema_slow_series = IndicatorCalculator.ema_series(prices, slow)

        # MACD line = EMA(fast) - EMA(slow), valid from index (slow - 1) onward
        macd_line: List[float] = []
        for f_val, s_val in zip(ema_fast_series, ema_slow_series):
            if math.isnan(f_val) or math.isnan(s_val):
                continue
            macd_line.append(f_val - s_val)

        if not macd_line:
            return {"macd": 0.0, "signal": None, "histogram": None}

        current_macd = macd_line[-1]

        # Signal line = EMA of MACD line
        if len(macd_line) >= signal:
            signal_series = IndicatorCalculator.ema_series(macd_line, signal)
            # Find the last valid signal value
            signal_value = signal_series[-1] if not math.isnan(signal_series[-1]) else None
            histogram = (
                current_macd - signal_value if signal_value is not None else None
            )
        else:
            signal_value = None
            histogram = None

        return {
            "macd": current_macd,
            "signal": signal_value,
            "histogram": histogram,
        }

    # ── Volume Ratio ─────────────────────────────────────────────────────

    @staticmethod
    def volume_ratio(volumes: List[int], period: int = 20) -> float:
        """
        Ratio of the latest volume to the average of the last *period* volumes.

        Returns
        -------
        float
            > 1.0 means above-average volume, < 1.0 below-average.
        """
        if len(volumes) < period + 1:
            raise ValueError(
                f"Volume ratio requires at least {period + 1} data points, "
                f"got {len(volumes)}"
            )
        avg_volume = sum(volumes[-(period + 1):-1]) / period
        if avg_volume == 0:
            return 0.0
        return volumes[-1] / avg_volume

    # ── Batch Computation ────────────────────────────────────────────────

    @staticmethod
    def calculate_all(daily_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Compute all available indicators from a list of daily OHLCV dicts.

        Parameters
        ----------
        daily_data : list[dict]
            Each dict must contain: ``open``, ``high``, ``low``, ``close``,
            ``volume``.  List should be in **chronological order** (oldest
            first).

        Returns
        -------
        dict
            All computed indicators keyed by name.  Indicators that cannot
            be computed due to insufficient data are set to ``None``.
        """
        if not daily_data:
            return {}

        closes = [d["close"] for d in daily_data]
        highs = [d["high"] for d in daily_data]
        lows = [d["low"] for d in daily_data]
        volumes = [d["volume"] for d in daily_data]
        n = len(closes)

        calc = IndicatorCalculator
        result: Dict[str, Any] = {}

        # ── Moving Averages ──────────────────────────────────────────
        for period in (5, 10, 20, 50, 150, 200):
            key = f"sma_{period}"
            try:
                result[key] = calc.sma(closes, period)
            except ValueError:
                result[key] = None

        for period in (5, 10, 20, 50):
            key = f"ema_{period}"
            try:
                result[key] = calc.ema(closes, period)
            except ValueError:
                result[key] = None

        # ── RSI ──────────────────────────────────────────────────────
        try:
            result["rsi_14"] = calc.rsi(closes, 14)
        except ValueError:
            result["rsi_14"] = None

        # ── VWAP (full dataset -- typically used intraday) ───────────
        try:
            result["vwap"] = calc.vwap(closes, volumes, highs, lows)
        except (ValueError, ZeroDivisionError):
            result["vwap"] = None

        # ── Bollinger Bands ──────────────────────────────────────────
        try:
            result["bollinger"] = calc.bollinger_bands(closes, 20, 2.0)
        except ValueError:
            result["bollinger"] = None

        # ── ATR ──────────────────────────────────────────────────────
        try:
            result["atr_14"] = calc.atr(highs, lows, closes, 14)
        except ValueError:
            result["atr_14"] = None

        # ── MACD ─────────────────────────────────────────────────────
        try:
            result["macd"] = calc.macd(closes, 12, 26, 9)
        except ValueError:
            result["macd"] = None

        # ── Volume Ratio ─────────────────────────────────────────────
        try:
            result["volume_ratio_20"] = calc.volume_ratio(volumes, 20)
        except ValueError:
            result["volume_ratio_20"] = None

        # ── Derived / convenience values ─────────────────────────────
        result["current_close"] = closes[-1] if closes else None
        result["current_volume"] = volumes[-1] if volumes else None
        result["data_points"] = n

        # MA200 slope (positive = rising) over the last 20 trading days
        if n >= 220:
            ma200_now = calc.sma(closes, 200)
            ma200_20_ago = calc.sma(closes[:-20], 200)
            result["ma200_slope"] = ma200_now - ma200_20_ago
        else:
            result["ma200_slope"] = None

        return result
