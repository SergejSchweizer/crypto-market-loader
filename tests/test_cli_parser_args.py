"""Parser-level coverage for individual CLI arguments."""

from __future__ import annotations

import pytest

from api.cli import build_parser


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["bronze-build", "--exchange", "deribit"], {"exchange": "deribit"}),
        (["bronze-build", "--exchanges", "deribit"], {"exchanges": ["deribit"]}),
        (["bronze-build", "--market", "perp_trades"], {"market": ["perp_trades"]}),
        (["bronze-build", "--market", "option_trades"], {"market": ["option_trades"]}),
        (["bronze-build", "--symbols", "BTC"], {"symbols": ["BTC"]}),
        (["bronze-build", "--perp-trade-symbols", "BTC", "ETH"], {"perp_trade_symbols": ["BTC", "ETH"]}),
        (["bronze-build", "--option-trade-symbols", "BTC"], {"option_trade_symbols": ["BTC"]}),
        (["bronze-build", "--save-parquet-lake"], {"save_parquet_lake": True}),
        (["bronze-build", "--lake-root", "lake/test-bronze"], {"lake_root": "lake/test-bronze"}),
        (["bronze-build", "--no-json-output"], {"no_json_output": True}),
        (["bronze-build", "--tail-delta-only"], {"tail_delta_only": True}),
        (["bronze-build", "--full-gap-fill"], {"tail_delta_only": False}),
        (["bronze-build", "--start-date", "2023-04-24"], {"start_date": "2023-04-24"}),
        (["bronze-build", "--symbol-start-dates", "BTC=2023-04-24"], {"symbol_start_dates": ["BTC=2023-04-24"]}),
        (
            ["bronze-build", "--exchange-symbol-start-dates", "deribit:BTC=2023-04-24"],
            {"exchange_symbol_start_dates": ["deribit:BTC=2023-04-24"]},
        ),
        (["silver-build", "--bronze-root", "lake/test-bronze"], {"bronze_root": "lake/test-bronze"}),
        (["silver-build", "--silver-root", "lake/test-silver"], {"silver_root": "lake/test-silver"}),
        (["silver-build", "--exchange", "deribit"], {"exchange": "deribit"}),
        (["silver-build", "--market", "spot"], {"market": ["spot"]}),
        (["silver-build", "--market", "option_trades"], {"market": ["option_trades"]}),
        (["silver-build", "--symbols", "BTC"], {"symbols": ["BTC"]}),
        (["silver-build", "--timeframe", "1m"], {"timeframe": "1m"}),
        (["silver-build", "--manifest"], {"manifest": True}),
        (["silver-build", "--plot"], {"plot": True}),
        (["silver-build", "--no-json-output"], {"no_json_output": True}),
        (["gold-build", "--silver-root", "lake/test-silver"], {"silver_root": "lake/test-silver"}),
        (["gold-build", "--gold-root", "lake/test-gold"], {"gold_root": "lake/test-gold"}),
        (["gold-build", "--l2-root", "lake/test-l2"], {"l2_root": "lake/test-l2"}),
        (["gold-build", "--exchange", "deribit"], {"exchange": "deribit"}),
        (["gold-build", "--symbols", "BTC"], {"symbols": ["BTC"]}),
        (["gold-build", "--dataset-id", "gold.market.full.m1"], {"dataset_id": "gold.market.full.m1"}),
        (["gold-build", "--dataset-id", "gold.market.trades.m1"], {"dataset_id": "gold.market.trades.m1"}),
        (["gold-build", "--dataset-id", "gold.market.option_trades.m1"], {"dataset_id": "gold.market.option_trades.m1"}),
        (["gold-build", "--dataset-version", "v1.2.3"], {"dataset_version": "v1.2.3"}),
        (["gold-build", "--auto-version"], {"auto_version": True}),
        (["gold-build", "--version-base", "v1.0.0"], {"version_base": "v1.0.0"}),
        (["gold-build", "--manifest"], {"manifest": True}),
        (["gold-build", "--plot"], {"plot": True}),
        (["gold-build", "--l2-validation-mode", "lenient"], {"l2_validation_mode": "lenient"}),
        (["gold-build", "--no-json-output"], {"no_json_output": True}),
        (["list-spot-timeframes", "--exchange", "deribit"], {"exchange": "deribit"}),
        (["list-spot-timeframes", "--exchanges", "deribit"], {"exchanges": ["deribit"]}),
        (["export-descriptive-stats", "--lake-root", "lake/test-bronze"], {"lake_root": "lake/test-bronze"}),
        (["export-descriptive-stats", "--output-csv", "docs/tables/out.csv"], {"output_csv": "docs/tables/out.csv"}),
        (
            ["export-descriptive-stats", "--start-time", "2026-01-01T00:00:00+00:00"],
            {"start_time": "2026-01-01T00:00:00+00:00"},
        ),
        (
            ["export-descriptive-stats", "--end-time", "2026-01-31T23:59:59+00:00"],
            {"end_time": "2026-01-31T23:59:59+00:00"},
        ),
        (["export-descriptive-stats", "--exchanges", "deribit"], {"exchanges": ["deribit"]}),
        (["export-descriptive-stats", "--symbols", "BTC"], {"symbols": ["BTC"]}),
        (["export-descriptive-stats", "--timeframes", "1m"], {"timeframes": ["1m"]}),
        (
            ["export-descriptive-stats", "--instrument-types", "spot"],
            {"instrument_types": ["spot"]},
        ),
        (["export-descriptive-stats", "--no-json-output"], {"no_json_output": True}),
    ],
)
def test_cli_argument_parsing_individual_arguments(argv: list[str], expected: dict[str, object]) -> None:
    """Each CLI argument must parse correctly in isolation."""

    parser = build_parser()
    args = parser.parse_args(argv)
    for field, value in expected.items():
        assert getattr(args, field) == value
