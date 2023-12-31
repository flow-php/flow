<?php declare(strict_types=1);

namespace Flow\ETL\Partition;

/**
 * @source https://stackoverflow.com/a/15973172
 */
final class CartesianProduct
{
    public function __invoke(array $input) : array
    {
        $result = [[]];

        foreach ($input as $key => $values) {
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
