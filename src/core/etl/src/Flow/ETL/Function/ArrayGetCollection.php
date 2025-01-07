<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ArrayDot\array_dot_get;
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;

final class ArrayGetCollection extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ScalarFunction|array $keys,
        private readonly ScalarFunction|string $index = '*',
    ) {
    }

    /**
     * @param array<string> $keys
     */
    public static function fromFirst(ScalarFunction $ref, ScalarFunction|array $keys) : self
    {
        return new self($ref, $keys, '0');
    }

    public function eval(Row $row) : mixed
    {
        try {
            $value = (new Parameter($this->ref))->asArray($row);
            $index = (new Parameter($this->index))->asString($row);
            $keys = (new Parameter($this->keys))->asArray($row);

            if ($value === null || $index === null || $keys === null) {
                return null;
            }

            $path = \sprintf("{$index}.{%s}", \implode(',', \array_map(fn (string $entryName) : string => '?' . $entryName, $keys)));

            try {
                $array = ($index === '0') ? \array_values($value) : $value;

                $extractedValues = array_dot_get($array, $path);
            } catch (InvalidPathException) {
                return null;
            }

            if (!\is_array($extractedValues)) {
                return null;
            }

            return $extractedValues;
        } catch (InvalidArgumentException) {
            return null;
        }
    }
}
