<?php

declare(strict_types=1);

namespace Flow\ArrayComparison;

/**
 * @psalm-immutable
 */
final class ArraySortByKey
{
    /**
     * @phpstan-ignore-next-line
     * @psalm-suppress MissingClosureParamType
     * @psalm-suppress MissingClosureReturnType
     */
    public function __invoke(array $array) : array
    {
        $array = \array_map(
            fn ($value) => \is_array($value) ? (new self)($value) : $value,
            $array
        );

        if ((new IsAssociativeArray)($array) === false) {
            \usort($array, fn ($a, $b) : int => $a <=> $b);
        } else {
            \uksort($array, fn ($a, $b) : int => $a <=> $b);
        }

        return $array;
    }
}
