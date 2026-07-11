<?php

declare(strict_types=1);

namespace Flow\Floe;

final readonly class EncoderPlan
{
    /**
     * @param array<array-key, EncoderColumn> $columns section columns keyed by name
     * @param string $schemaBody SCHEMA frame body describing this plan's columns
     */
    public function __construct(
        public array $columns,
        public string $schemaBody,
    ) {}
}
