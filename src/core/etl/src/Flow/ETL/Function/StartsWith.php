<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\type_string;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Row;

final class StartsWith extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $haystack,
        private readonly ScalarFunction $needle
    ) {
    }

    public function eval(Row $row) : bool
    {
        $haystack = Caster::default()->to(type_string(true))->value($this->haystack->eval($row));
        $needle = Caster::default()->to(type_string(true))->value($this->needle->eval($row));

        if (!\is_string($needle) || !\is_string($haystack)) {
            return false;
        }

        return \str_starts_with($haystack, $needle);
    }
}
