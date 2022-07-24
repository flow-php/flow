<?php

declare(strict_types=1);

namespace Flow\ETL\Loader\StreamLoader;

enum Output
{
    case rows;
    case rows_and_schema;
    case schema;
}
