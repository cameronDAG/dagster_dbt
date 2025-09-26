import dagster as dg
from dagster_and_dbt.defs.assets.dbt import dbt_analytics
from dagster_dbt import build_dbt_asset_selection

from dagster_and_dbt.defs.partitions import monthly_partition, weekly_partition, daily_partition

# Seleksi aset-aset berdasarkan nama
trips_by_week = dg.AssetSelection.assets("trips_by_week")
adhoc_request = dg.AssetSelection.assets("adhoc_request")
daily_metrics = dg.AssetSelection.assets("daily_metrics")

# Seleksi aset dari dbt yang tergantung pada stg_trips
dbt_trips_selection = build_dbt_asset_selection([dbt_analytics], "stg_trips+")

# Job bulanan untuk semua asset kecuali yang tidak sesuai
trip_update_job = dg.define_asset_job(
    name="trip_update_job",
    partitions_def=monthly_partition,
    selection=(
        dg.AssetSelection.all()
        - trips_by_week
        - adhoc_request
        - dbt_trips_selection
        - daily_metrics  # 🛑 Tambahkan ini supaya daily_metrics tidak ikut ke monthly job
    )
)

# Job mingguan untuk trips_by_week
weekly_update_job = dg.define_asset_job(
    name="weekly_update_job",
    partitions_def=weekly_partition,
    selection=trips_by_week
)

# Job non-partisi untuk permintaan adhoc
adhoc_request_job = dg.define_asset_job(
    name="adhoc_request_job",
    selection=adhoc_request
)

# ✅ Job baru khusus untuk daily_metrics
daily_metrics_job = dg.define_asset_job(
    name="daily_metrics_job",
    partitions_def=daily_partition,
    selection=daily_metrics
)
