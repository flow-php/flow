<?php

declare(strict_types=1);

namespace Flow\ArrayComparison;

final class ArraySortByKey
{
    /**
     * @param array<mixed> $array
     *
     * @return array<mixed>
     */
    public function __invoke(array $array) : array
    {
        $array = \array_map(
            fn ($value) => \is_array($value) ? (new self)($value) : $value,
            $array
        );

        if (\array_is_list($array)) {
            \sort($array);
        } else {
            \ksort($array);
        }

        return $array;
    }
}
