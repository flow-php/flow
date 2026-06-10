<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

enum Operation: string
{
    case DELETE = 'delete';
    case UPSERT = 'upsert';
}
