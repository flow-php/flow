<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

final readonly class RenameCandidate
{
    public function __construct(
        public string $addedName,
        public string $removedName,
    ) {}
}
