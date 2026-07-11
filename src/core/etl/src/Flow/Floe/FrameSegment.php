<?php

declare(strict_types=1);

namespace Flow\Floe;

final readonly class FrameSegment
{
    public function __construct(
        public ?string $schemaBody,
        public string $frames,
        public int $rowCount,
    ) {}
}
