from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    ScheduleDefinition,
)

import Test_OEE.assets as accuracy_module

all_assets = load_assets_from_modules([accuracy_module])

oee_daily_job = define_asset_job(
    name="OEE_TAMPO_daily",
    selection="*",
)

daily_schedule = ScheduleDefinition(
    job=oee_daily_job,
    cron_schedule="0 15 * * *",
)

defs = Definitions(
    assets=all_assets,
    jobs=[oee_daily_job],
    schedules=[daily_schedule],
)