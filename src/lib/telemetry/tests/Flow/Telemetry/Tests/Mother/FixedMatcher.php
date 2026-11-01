<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Filter\Matcher;

final readonly class FixedMatcher implements Matcher
{
    public function __construct(
        private bool $result,
    ) {}

    public function matches(Attributes $attributes): bool
    {
        return $this->result;
    }
}
