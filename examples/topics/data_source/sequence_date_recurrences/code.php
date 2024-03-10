<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_sequence_date_period_recurrences, to_output};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_sequence_date_period_recurrences(
        'date',
        new DateTimeImmutable('now'),
        new DateInterval('P1D'),
        recurrences: 60
    ))
    ->write(to_output(false))
    ->run();
