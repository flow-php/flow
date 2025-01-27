<?php

use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;
use function Flow\ETL\DSL\df;

df()
    ->write(
        to_dbal_table_insert(
            $connection,
            'user_addresses',
            [
                'conflict_columns' => ['id']
            ]
        )
    )
;