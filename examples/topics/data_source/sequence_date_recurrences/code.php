<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_sequence_date_period_recurrences, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_sequence_date_period_recurrences(
        'date',
        new DateTimeImmutable('now'),
        new DateInterval('P1D'),
        recurrences: 60
    ))
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
