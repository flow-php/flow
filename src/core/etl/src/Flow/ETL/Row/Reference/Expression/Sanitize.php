<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Sanitize implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly Expression $placeholder,
        private readonly Expression $skipCharacters
    ) {
    }

    public function eval(Row $row) : ?string
    {
        /** @var mixed $val */
        $val = $this->ref->eval($row);

        if (!\is_string($val)) {
            return null;
        }

        /** @var mixed $placeholder */
        $placeholder = $this->placeholder->eval($row);

        /** @var mixed $skipCharacters */
        $skipCharacters = $this->skipCharacters->eval($row);

        $size = \mb_strlen($val);

        if (0 !== $skipCharacters) {
            if ($size > $skipCharacters) {
                $cut = \mb_substr($val, 0, $skipCharacters);

                return $cut . \str_repeat($placeholder, $size - $skipCharacters);
            }
        }

        return \str_repeat($placeholder, $size);
    }
}
