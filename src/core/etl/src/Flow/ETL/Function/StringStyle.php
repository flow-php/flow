<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_enum, type_string};
use Flow\ETL\Function\StyleConverter\StringStyles;
use Flow\ETL\Row;

final class StringStyle extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $string,
        private readonly ScalarFunction|string|StringStyles $style,
    ) {
    }

    public function eval(Row $row) : ?string
    {

        $string = (new Parameter($this->string))->asString($row);
        $style = (new Parameter($this->style))->as($row, type_string(), type_enum(StringStyles::class));

        if ($string === null || $style === null) {
            return null;
        }

        if (is_string($style)) {
            $style = StringStyles::fromString($style);
        }

        return $style->convert($string);
    }
}
