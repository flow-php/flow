<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\type_string;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Row;

final class StrPad extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly int $length,
        private readonly string $pad_string = ' ',
        private readonly int $type = STR_PAD_RIGHT
    ) {
    }

    public function eval(Row $row) : mixed
    {
        /** @var null|string $val */
        $val = Caster::default()->to(type_string(true))->value($this->ref->eval($row));

        if (!\is_string($val)) {
            return null;
        }

        return \str_pad($val, $this->length, $this->pad_string, $this->type);
    }
}
