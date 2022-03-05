<?php

declare(strict_types=1);

namespace Flow\ArrayComparison;

/**
 * @psalm-immutable
 */
final class IsAssociativeArray
{
    /**
     * @param array<mixed> $array
     *
     * @return bool
     */
    public function __invoke(array $array) : bool
    {
        foreach (\array_keys($array) as $key) {
            if (!\is_int($key)) {
                return true;
            }
        }

        return false;
    }
}
