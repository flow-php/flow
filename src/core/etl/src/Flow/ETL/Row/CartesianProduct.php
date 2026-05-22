<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use function is_iterable;

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
    public function __invoke(array $input): array
    {
        $result = [[]];

        // @mago-ignore analysis:mixed-assignment
        foreach ($input as $key => $values) {
            if (!is_iterable($values)) {
                continue;
            }

            $append = [];

            foreach ($result as $product) {
                // @mago-ignore analysis:mixed-assignment
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
