<?php

declare(strict_types=1);

namespace Flow\ArrayComparison;

final class ArrayComparison
{
    /**
     * @param array<mixed> $a
     * @param array<mixed> $b
     */
    public function compare(array $a, array $b) : int
    {
        return (new ArraySortByKey)($a) <=> (new ArraySortByKey)($b);
    }

    /**
     * @param array<mixed> $a
     * @param array<mixed> $b
     */
    public function equals(array $a, array $b) : bool
    {
        return $this->valueEquals((new ArraySortByKey)($a), (new ArraySortByKey)($b));
    }

    /**
     * @param mixed $a
     * @param mixed $b
     */
    private function valueEquals($a, $b) : bool
    {
        if (!\is_array($b) || !\is_array($a)) {
            if (\is_object($a) && \is_object($b)) {
                return $a == $b;
            }

            return $a === $b;
        }

        if (\count($a) !== \count($b)) {
            return false;
        }

        /** @psalm-suppress MixedAssignment */
        foreach ($b as $key => $value) {
            if (!\array_key_exists($key, $a)) {
                return false;
            }

            if (!$this->valueEquals($a[$key], $value)) {
                return false;
            }
        }

        return true;
    }
}
