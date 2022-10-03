<?php

declare(strict_types=1);

use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\To;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Flow\ETL\GroupBy\Aggregation;

require __DIR__ . '/../../bootstrap.php';

(new Flow())
    ->read(CSV::from(__FLOW_DATA__ . '/power-plant-daily.csv', 10, delimiter: ';'))
    ->rows(Transform::array_unpack('row'))
    ->rows(Transform::rename('Produkcja(kWh)', 'production_kwh'))
    ->rows(Transform::rename('Zużycie(kWh)', 'consumption_kwh'))
    ->rows(Transform::rename('Zaktualizowany czas', 'date'))
    ->rows(Transform::to_datetime_from_string(['date']))
    ->rows(Transform::to_string_from_datetime(['date'], 'Y/m'))
    ->select('date', 'production_kwh', 'consumption_kwh')
    ->groupBy('date')
    ->aggregate(
        Aggregation::avg('production_kwh'),
        Aggregation::avg('consumption_kwh'),
        Aggregation::min('production_kwh'),
        Aggregation::min('consumption_kwh'),
        Aggregation::max('production_kwh'),
        Aggregation::max('consumption_kwh'),
        Aggregation::sum('production_kwh'),
        Aggregation::sum('consumption_kwh')
    )
    ->rows(
        Transform::to_integer(
            'production_kwh_avg',
            'consumption_kwh_avg',
            'production_kwh_min',
            'consumption_kwh_min',
            'production_kwh_max',
            'consumption_kwh_max',
            'production_kwh_sum',
            'consumption_kwh_sum'
        )
    )
    ->rows(Transform::divide('consumption_kwh_sum', 'production_kwh_sum', 'consumption'))
    ->rows(Transform::multiply_by('consumption', 100))
    ->rows(Transform::round('consumption', 2))
    ->rows(Transform::suffix('consumption', '%'))
    ->write(To::output(truncate: false))
    ->run();
