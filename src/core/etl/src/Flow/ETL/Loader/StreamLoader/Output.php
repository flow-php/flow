<?php

declare(strict_types=1);

namespace Flow\ETL\Loader\StreamLoader;

enum Output
{
    case column_count;
    case rows;
    case rows_and_column_count;
    case rows_and_schema;
    case rows_count;
    case schema;
}
