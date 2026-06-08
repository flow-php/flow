<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

/**
 * Mutable context threaded through {@see CompilableMatcher::compile()}.
 */
final class Compilation
{
    private int $counter = 0;

    public function __construct(
        public readonly string $subject,
        public readonly string $values,
    ) {}

    public function temp(): string
    {
        return '$__m' . $this->counter++;
    }
}
