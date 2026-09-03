<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

interface PlaceholderDialect
{
    /**
     * @param int<1, max> $position
     */
    public function placeholder(int $position): string;

    /**
     * Whether the driver reads a backslash inside a string literal as an escape, which decides how
     * DBAL's parser tokenises string literals.
     */
    public function usesBackslashEscapes(): bool;
}
