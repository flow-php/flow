<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_branch;

$report = df()
    ->read(from_csv(__DIR__ . '/import.csv'))
    ->with(new Validation())
    ->write(
        to_branch(
            ref('valid')->isFalse(),
            to_csv(__DIR__ . '/invalid_rows_' . time() . '.csv'),
        )
    )
    ->filter(ref('valid')->isTrue())
    ->drop('valid')
    ->batchSize(100)
    ->joinEach(
        new UserIdJoinDataFrameFactory($connection),
        join_on(['user_email' => 'user_email'])
    )
    ->drop('user_email')
;