<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Trigger;

enum TriggerLevel: string
{
    case ROW = 'ROW';
    case STATEMENT = 'STATEMENT';

    public function toBool(): bool
    {
        return $this === self::ROW;
    }
}
