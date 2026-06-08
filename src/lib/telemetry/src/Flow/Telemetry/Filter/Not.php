<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

use Flow\Telemetry\Attributes;

/**
 * Matches when the nested matcher does not match (logical NOT).
 */
final readonly class Not implements CompilableMatcher
{
    public function __construct(
        private Matcher $matcher,
    ) {}

    public function matches(Attributes $attributes): bool
    {
        return !$this->matcher->matches($attributes);
    }

    public function compile(Compilation $compilation): string
    {
        if (!$this->matcher instanceof CompilableMatcher) {
            throw new NotCompilable('Not wraps a matcher that cannot be compiled');
        }

        return '!(' . $this->matcher->compile($compilation) . ')';
    }
}
