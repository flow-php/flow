<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ArrayDot\array_dot_get;
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;

final class ArrayGetCollection implements ScalarFunction
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly array $keys,
        private readonly string $index = '*',
    ) {
    }

    /**
     * @param array<string> $keys
     */
    public static function fromFirst(ScalarFunction $ref, array $keys) : self
    {
        return new self($ref, $keys, '0');
    }

    public function eval(Row $row) : mixed
    {
        try {
            /** @var mixed $value */
            $value = $this->ref->eval($row);

            if (!\is_array($value)) {
                return null;
            }

            $path = \sprintf("{$this->index}.{%s}", \implode(',', \array_map(fn (string $entryName) : string => '?' . $entryName, $this->keys)));

            try {
                $array = ($this->index === '0') ? \array_values($value) : $value;

                $extractedValues = array_dot_get($array, $path);
            } catch (InvalidPathException) {
                return null;
            }

            if (!\is_array($extractedValues)) {
                return null;
            }

            return $extractedValues;
        } catch (InvalidArgumentException $e) {
            return null;
        }
    }
}
