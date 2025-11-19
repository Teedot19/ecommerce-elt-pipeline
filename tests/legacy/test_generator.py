from datetime import date

from ingestion.generators.generator import EcommerceDataGenerator


def test_generator_writes_expected_files(temp_dir):
    gen = EcommerceDataGenerator(
        output_path=str(temp_dir),
        daily_rows={"customers": 2, "products": 1, "orders": 3},
        initial_rows={"customers": 10, "products": 5, "orders": 20},
    )

    run_dt = date(2025, 11, 7)
    gen.run_incremental_batch(run_dt)

    folder = temp_dir / run_dt.isoformat()
    assert folder.exists()

    csvs = list(folder.glob("*.csv"))
    # customers, products, orders, order_items, payments
    assert len(csvs) == 5
