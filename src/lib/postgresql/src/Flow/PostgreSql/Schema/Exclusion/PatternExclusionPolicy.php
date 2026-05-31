<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Exclusion;

use InvalidArgumentException;

use function preg_match;
use function sprintf;

final readonly class PatternExclusionPolicy implements ExclusionPolicy
{
    /**
     * @param string $pattern PCRE pattern, including delimiters (e.g. '/^cache_\d+$/')
     */
    public function __construct(
        private string $pattern,
    ) {
        if (@preg_match($this->pattern, '') === false) {
            throw new InvalidArgumentException(sprintf('Invalid exclusion pattern: "%s"', $this->pattern));
        }
    }

    public function exclude(SchemaObject $object): bool
    {
        return $object->name !== null && preg_match($this->pattern, $object->name) === 1;
    }
}
