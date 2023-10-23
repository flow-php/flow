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

        $placeholder = (string) $this->placeholder->eval($row);
        $skipCharacters = (int) $this->skipCharacters->eval($row);

        $size = \mb_strlen($val);

        if (0 !== $skipCharacters && $size > $skipCharacters) {
            return \mb_substr($val, 0, $skipCharacters) . \str_repeat($placeholder, $size - $skipCharacters);
        }

        return \str_repeat($placeholder, $size);
    }
}
