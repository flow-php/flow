<?php declare(strict_types=1);

namespace Flow\Dremel;

use Flow\Parquet\Exception\RuntimeException;

final class DataShredded
{
    public function __construct(
        public readonly array $repetitions,
        public readonly array $definitions,
        public readonly array $values
    ) {
    }

    public function indices(array $dictionary) : array
    {
        $indices = [];

        foreach ($this->values as $value) {
            $index = \array_search($value, $dictionary, true);

            if (!\is_int($index)) {
                throw new RuntimeException('Value "' . $value . '" not found in dictionary');
            }

            $indices[] = $index;
        }

        return $indices;
    }
}
