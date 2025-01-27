<?php

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\when;

df()
    ->withEntry(
        'valid',
        when(ref('user_email')->isType(type_string())
            ->and(ref('user_email')->size()->between(1, 256)), ref('valid'), lit(false))
    );