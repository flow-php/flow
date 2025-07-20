<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\type_integer;
use function Symfony\Component\String\u;
use Flow\ETL\Row;

final class IndexOfLast extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $string,
        private readonly ScalarFunction|string $needle,
        private readonly ScalarFunction|bool $ignoreCase = false,
        private readonly ScalarFunction|int $offset = 0,
    ) {
    }

    public function eval(Row $row) : int|false|null
    {
        $string = (new Parameter($this->string))->asString($row);
        $needle = (new Parameter($this->needle))->asString($row);
        $offset = type_integer()->assert((new Parameter($this->offset))->as($row, type_integer()));
        $ignoreCase = (new Parameter($this->ignoreCase))->asBoolean($row);

        if ($string === null || $needle === null) {
            return false;
        }

        if ($ignoreCase) {
            return u($string)->ignoreCase()->indexOfLast($needle, $offset);
        }

        return u($string)->indexOfLast($needle, $offset);
    }
}
