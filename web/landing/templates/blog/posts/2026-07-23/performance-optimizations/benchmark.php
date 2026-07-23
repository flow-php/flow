<?php

use function Flow\ETL\DSL\df;

df()
    ->read(from_xxx())
    ->batchSize(1000)
    ->run();