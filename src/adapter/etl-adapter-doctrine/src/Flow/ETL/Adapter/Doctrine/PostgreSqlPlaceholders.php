<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

final readonly class PostgreSqlPlaceholders implements PlaceholderDialect
{
    public function placeholder(int $position): string
    {
        return '$' . $position;
    }

    public function usesBackslashEscapes(): bool
    {
        return false;
    }
}
