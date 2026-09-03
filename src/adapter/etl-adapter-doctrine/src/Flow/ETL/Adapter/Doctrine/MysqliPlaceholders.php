<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

final readonly class MysqliPlaceholders implements PlaceholderDialect
{
    public function placeholder(int $position): string
    {
        return '?';
    }

    public function usesBackslashEscapes(): bool
    {
        return true;
    }
}
