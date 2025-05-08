<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_enum, type_string};
use Flow\ETL\Function\StyleConverter\StringStyles as OldStringStyles;
use Flow\ETL\Row;
use Flow\ETL\String\StringStyles;

final class StringStyle extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $string,
        private readonly ScalarFunction|string|OldStringStyles|StringStyles $style,
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $string = (new Parameter($this->string))->asString($row);
        $style = (new Parameter($this->style))->as($row, type_string(), type_enum(StringStyles::class), type_enum(OldStringStyles::class));

        if ($string === null || $style === null) {
            return null;
        }

        if (is_string($style)) {
            $style = StringStyles::fromString($style);
        } elseif ($style instanceof OldStringStyles) {
            $style = StringStyles::fromString($style->value);
        }

        return $style->convert($string);
    }
}
