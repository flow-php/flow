<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

final readonly class DependentViews
{
    /**
     * @param list<DependentView> $toDrop views to drop before table modifications, ordered most-dependent first
     * @param list<DependentView> $toCreate views to recreate after table modifications, ordered least-dependent first
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
