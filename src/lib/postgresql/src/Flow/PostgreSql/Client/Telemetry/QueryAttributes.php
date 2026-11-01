<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Telemetry;

final readonly class QueryAttributes
{
    public function __construct(
        public ?string $operation,
        public ?string $target,
    ) {}
}
