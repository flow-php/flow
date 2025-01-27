<?php

use Ramsey\Uuid\Uuid;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_branch;
use function Flow\ETL\DSL\when;

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
    // at this point all invalid records are stored in another file
    ->drop('valid')
    // we need to extract from the database user_id based on user_email
    // lets do it in batches of 100
    ->batchSize(100)
    ->joinEach(
        new UserIdJoinDataFrameFactory($connection),
        join_on(['user_email' => 'user_email'])
    )
    // user email is no longer needed
    ->drop('user_email')
    // defines the batch size for the insert operation
    ->batchSize(100)
    // when import file does not have address id we need to generate
    ->withEntry('id', when(ref('id')->isNull(), lit(Uuid::uuid4()->toString()), ref('id')))
    ->write(
        to_dbal_table_insert(
            $connection,
            'user_addresses',
            [
                'conflict_columns' => ['id']
            ]
        )
    )
    ->run(analyze: true);


echo 'Total rows: ' . $report->statistics()->totalRows() . PHP_EOL;
echo 'Execution time: ' . $report->statistics()->executionTime->highResolutionTime->toString() . PHP_EOL;