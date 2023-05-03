<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Sanitize implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly string $placeholder,
        private readonly int $charactersLeft
    ) {
    }

    public function eval(Row $row) : ?string
    {
        /** @var mixed $val */
        $val = $this->ref->eval($row);

        if (!\is_string($val)) {
            return null;
        }

        $size = \strlen($val);

        if (0 !== $this->charactersLeft) {
            if ($size > $this->charactersLeft) {
                $cut = \substr($val, 0, $this->charactersLeft);

                return $cut . \str_repeat($this->placeholder, $size - $this->charactersLeft);
            }
        }

        return \str_repeat($this->placeholder, $size);
    }
}
