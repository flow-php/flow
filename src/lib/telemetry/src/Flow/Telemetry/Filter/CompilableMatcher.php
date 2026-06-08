<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

/**
 * A {@see Matcher} that can render itself as an inlined PHP boolean expression,
 * letting {@see CompiledMatcher} compile the whole matcher tree to a cached PHP
 * file instead of evaluating it through {@see Matcher::matches()}.
 */
interface CompilableMatcher extends Matcher
{
    /**
     * Render this matcher as a PHP boolean expression operating on the attribute
     * variable named by {@see Compilation::$subject} (e.g. '$a').
     *
     * @throws NotCompilable when this matcher (or a nested matcher) cannot be
     *                       represented as inlined PHP; the filter then falls
     *                       back to interpreted matching
     */
    public function compile(Compilation $compilation): string;
}
