<?php

declare(strict_types=1);

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;
use Flow\ETL\DSL\Parquet;
use Flow\ETL\DSL\To;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;

require __DIR__ . '/../../bootstrap.php';

$flow = (new Flow())
    ->read(Parquet::from(__FLOW_DATA__ . '/orders_flow.parquet'))
    ->select('created_at', 'total_price', 'discount')
    ->withEntry('created_at', ref('created_at')->toDate()->dateFormat('Y/m'))
    ->withEntry('revenue', ref('total_price')->minus(ref('discount')))
    ->select('created_at', 'revenue')
    ->groupBy('created_at')
    ->aggregate(sum(ref('revenue')))
    ->sortBy(ref('created_at')->desc())
    ->withEntry('daily_revenue', ref('revenue_sum')->round(lit(2))->numberFormat(lit(2)))
    ->drop('revenue_sum')
    ->write(To::output(truncate: false))
    ->withEntry('created_at', ref('created_at')->toDate('Y/m'))
    ->mode(SaveMode::Overwrite)
    ->write(Parquet::to(__FLOW_OUTPUT__ . '/daily_revenue.parquet'));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$flow->run();
