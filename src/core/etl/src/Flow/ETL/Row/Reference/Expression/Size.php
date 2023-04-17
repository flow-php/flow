<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Size implements Expression
{
    public function __construct(private readonly Expression $ref)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        return match (\gettype($value)) {
            'array' => \count($value),
            'string' => \mb_strlen($value),
            default => throw new RuntimeException('Cannot get size of value ' . \gettype($value)),
        };
    }
}
