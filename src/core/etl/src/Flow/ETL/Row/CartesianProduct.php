<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

/**
 * @source https://stackoverflow.com/a/15973172
 */
final class CartesianProduct
{
    /**
     * @param array<array-key, mixed> $input
     *
     * @return array<array-key, mixed>
     */
    public function __invoke(array $input) : array
    {
        $result = [[]];

        foreach ($input as $key => $values) {
            if (!\is_iterable($values)) {
                continue;
            }

            $append = [];

            foreach ($result as $product) {
                foreach ($values as $item) {
                    $product[$key] = $item;
                    $append[] = $product;
                }
            }

            $result = $append;
        }

        return $result;
    }
}
