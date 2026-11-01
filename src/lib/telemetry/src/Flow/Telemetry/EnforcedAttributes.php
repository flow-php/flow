<?php

declare(strict_types=1);

namespace Flow\Telemetry;

final readonly class EnforcedAttributes
{
    public function __construct(
        public Attributes $attributes,
        public int $droppedAttributeCount,
    ) {}
}
