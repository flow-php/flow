<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

use Flow\Telemetry\Attributes;

/**
 * Also implement CompilableMatcher to let AttributeFilter inline it.
 */
interface Matcher
{
    public function matches(Attributes $attributes): bool;
}
