<?php

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\to_output;
use function Flow\ETL\Adapter\GoogleAnalytics\from_ga_account_summaries;

df()
    ->read(from_ga_account_summaries($client))
    ->limit(2)
    ->collect()
    ->write(to_output())
    ->run();