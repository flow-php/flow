<?php

use Flow\ETL\Runtime\Heartbeat\HeartbeatFailure;
use function Flow\Bridge\Symfony\Console\DSL\console_heartbeat;
use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{config_builder, df, ref};

$progress = console_heartbeat($output);

try {
    df(config_builder()->heartbeat($progress, everySeconds: 0.25, onFailure: HeartbeatFailure::log))
        ->read(from_csv($path))
        ->sortBy(ref('id'))
        ->write(to_csv($out))
        ->run();
} finally {
    $progress->finish();
}
