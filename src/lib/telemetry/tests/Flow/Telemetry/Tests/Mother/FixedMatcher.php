<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Filter\Matcher;

/**
 * A matcher that always returns a fixed result. Implements only {@see Matcher}
 * (not CompilableMatcher), so it exercises the interpreted path and the
 * not-compilable branches of the composite matchers.
 */
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
