<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_int, type_string};
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Row;

final class Sanitize extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ScalarFunction $placeholder,
        private readonly ScalarFunction $skipCharacters
    ) {
    }

    public function eval(Row $row) : ?string
    {
        /** @var mixed $val */
        $val = $this->ref->eval($row);

        if (!\is_string($val)) {
            return null;
        }

        $placeholder = Caster::default()->to(type_string(true))->value($this->placeholder->eval($row));
        $skipCharacters = Caster::default()->to(type_int(true))->value($this->skipCharacters->eval($row));

        $size = \mb_strlen($val);

        if (0 !== $skipCharacters && $size > $skipCharacters) {
            return \mb_substr($val, 0, $skipCharacters) . \str_repeat($placeholder, $size - $skipCharacters);
        }

        return \str_repeat($placeholder, $size);
    }
}
