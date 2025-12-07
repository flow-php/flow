<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Copy;

enum CopyFormat : string
{
    case BINARY = 'binary';
    case CSV = 'csv';
    case TEXT = 'text';
}
