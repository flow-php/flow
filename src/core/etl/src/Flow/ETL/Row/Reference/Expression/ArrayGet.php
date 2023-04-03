<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use function Flow\ArrayDot\array_dot_get;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class ArrayGet implements Expression
{
    public function __construct(
        private readonly Expression $reference,
        private readonly string $path
    ) {
    }

    public function eval(Row $row) : mixed
    {
        try {
            /** @var mixed $value */
            $value = (new Row\Reference\ValueExtractor())->value($row, $this->reference);

            if (!\is_array($value)) {
                return null;
            }

            return array_dot_get($value, $this->path);
        } catch (InvalidArgumentException $e) {
            return null;
        }
    }
}
