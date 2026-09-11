<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

final readonly class NodeFixture
{
    public function __construct(
        private NodeSource $source,
        private int $rows,
    ) {}

    public function warm(): void
    {
        $this->source->path($this->rows);
    }
}
