<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

final readonly class NoRenameStrategy implements RenameStrategy
{
    public function resolve(array $structuralMatches): array
    {
        return [];
    }
}
