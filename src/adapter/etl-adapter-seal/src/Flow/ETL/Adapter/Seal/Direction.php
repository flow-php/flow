<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

enum Direction: string
{
    case ASC = 'asc';
    case DESC = 'desc';
}
