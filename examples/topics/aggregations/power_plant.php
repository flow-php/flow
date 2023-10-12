<?php

declare(strict_types=1);

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\struct;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\GroupBy\Aggregation;

require __DIR__ . '/../../bootstrap.php';

$flow = (new Flow())
    ->read(CSV::from(__FLOW_DATA__ . '/power-plant-daily.csv', 10, delimiter: ';'))
    ->withEntry('production_kwh', ref('Produkcja(kWh)'))
    ->withEntry('consumption_kwh', ref('Zużycie(kWh)'))
    ->withEntry('date', ref('Zaktualizowany czas')->toDate('Y/m/d')->dateFormat('Y/m'))
    ->select(struct('date', 'production_kwh', 'consumption_kwh'))
    ->groupBy(ref('date'))
    ->aggregate(
        Aggregation::avg(ref('production_kwh')),
        Aggregation::avg(ref('consumption_kwh')),
        Aggregation::min(ref('production_kwh')),
        Aggregation::min(ref('consumption_kwh')),
        Aggregation::max(ref('production_kwh')),
        Aggregation::max(ref('consumption_kwh')),
        Aggregation::sum(ref('production_kwh')),
        Aggregation::sum(ref('consumption_kwh'))
    )

    ->withEntry('production_kwh_avg', ref('production_kwh_avg')->round(lit(2)))
    ->withEntry('consumption_kwh_avg', ref('consumption_kwh_avg')->round(lit(2)))
    ->withEntry('production_kwh_min', ref('production_kwh_min')->round(lit(2)))
    ->withEntry('consumption_kwh_min', ref('consumption_kwh_min')->round(lit(2)))
    ->withEntry('production_kwh_max', ref('production_kwh_max')->round(lit(2)))
    ->withEntry('consumption_kwh_max', ref('consumption_kwh_max')->round(lit(2)))
    ->withEntry('production_kwh_sum', ref('production_kwh_sum')->round(lit(2)))
    ->withEntry('consumption_kwh_sum', ref('consumption_kwh_sum')->round(lit(2)))
    ->withEntry('consumption', ref('consumption_kwh_sum')->divide(ref('production_kwh_sum')))
    ->withEntry('consumption', ref('consumption')->multiply(lit(100))->round(lit(2)))
    ->withEntry('consumption', concat(ref('consumption'), lit('%')))
    ->write(To::output(truncate: false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$flow->run();
