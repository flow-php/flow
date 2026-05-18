<?php

declare(strict_types=1);

namespace Flow\ArrayComparison;

use function array_is_list;
use function array_map;
use function is_array;
use function ksort;
use function sort;

final class ArraySortByKey
{
    /**
     * @param array<mixed> $array
     *
     * @return array<mixed>
     */
    public function __invoke(array $array): array
    {
        $array = array_map(static fn($value) => is_array($value) ? (new self())($value) : $value, $array);

        if (array_is_list($array)) {
            sort($array);
        } else {
            ksort($array);
        }

        return $array;
    }
}
