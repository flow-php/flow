<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class ArrayMergeCollection implements Expression
{
    public function __construct(private readonly Expression $ref)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        if (!\is_array($value)) {
            return null;
        }

        foreach ($value as $index => $element) {
            if (!\is_array($element)) {
                $type = \gettype($element);

                throw new RuntimeException(\get_class($this->ref) . " must be an array of arrays, instead element at position \"{$index}\" is {$type}");
            }
        }

        /** @psalm-suppress MixedArgument */
        return \array_merge(...\array_values($value));
    }
}
