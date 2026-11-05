<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Schema\MaterializedView;
use Flow\PostgreSql\Schema\View;

final readonly class DependentViews
{
    /**
     * @param list<MaterializedView|View> $toDrop views to drop before table modifications, ordered most-dependent first
     * @param list<MaterializedView|View> $toCreate views to recreate after table modifications, ordered least-dependent first
     */
    public function __construct(
        public array $toDrop,
        public array $toCreate,
    ) {}

    public static function empty(): self
    {
        return new self([], []);
    }

    public function isEmpty(): bool
    {
        return $this->toDrop === [] && $this->toCreate === [];
    }
}
