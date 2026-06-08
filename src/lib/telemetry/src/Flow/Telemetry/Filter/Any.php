<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

use Flow\Telemetry\Attributes;
use InvalidArgumentException;

use function array_values;
use function implode;

/**
 * Matches when at least one nested matcher matches (logical OR).
 */
final readonly class Any implements CompilableMatcher
{
    /**
     * @var non-empty-list<Matcher>
     */
    private array $matchers;

    public function __construct(Matcher ...$matchers)
    {
        if ($matchers === []) {
            throw new InvalidArgumentException('Any requires at least one matcher');
        }

        $this->matchers = array_values($matchers);
    }

    public function matches(Attributes $attributes): bool
    {
        foreach ($this->matchers as $matcher) {
            if ($matcher->matches($attributes)) {
                return true;
            }
        }

        return false;
    }

    public function compile(Compilation $compilation): string
    {
        $parts = [];

        foreach ($this->matchers as $matcher) {
            if (!$matcher instanceof CompilableMatcher) {
                throw new NotCompilable('Any contains a matcher that cannot be compiled');
            }

            $parts[] = '(' . $matcher->compile($compilation) . ')';
        }

        return implode(' || ', $parts);
    }
}
