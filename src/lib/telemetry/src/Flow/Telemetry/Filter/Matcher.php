<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

use Flow\Telemetry\Attributes;

/**
 * A predicate over a signal's attributes.
 *
 * Leaf matchers ({@see AttributeRule}) test a single attribute; composite
 * matchers ({@see All}, {@see Any}, {@see Not}) combine other matchers. An
 * {@see AttributeFilter} drops or keeps signals based on a single root matcher.
 *
 * Implement this interface to add a custom matcher. Matchers that can also be
 * represented as inlined PHP additionally implement {@see CompilableMatcher} so
 * the filter can compile them; matchers that only implement {@see self} are
 * always evaluated through {@see self::matches()}.
 */
interface Matcher
{
    public function matches(Attributes $attributes): bool;
}
