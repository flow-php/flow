<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\{type_enum, type_string};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};
use Flow\ETL\Function\StyleConverter\StringStyles as OldStringStyles;
use Flow\ETL\String\StringStyles;

final class StringStyle extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $string,
        private readonly ScalarFunction|string|OldStringStyles|StringStyles $style,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?string
    {
        $string = (new Parameter($this->string))->asString($row, $context);
        $style = (new Parameter($this->style))->as($row, $context, type_string(), type_enum(StringStyles::class), type_enum(OldStringStyles::class));

        if ($string === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('StringStyle function requires non-null value'));
        }

        if ($style === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('StringStyle function requires non-null style'));
        }

        if (is_string($style)) {
            $style = StringStyles::fromString($style);
        } elseif ($style instanceof OldStringStyles) {
            $style = StringStyles::fromString($style->value);
        }

        if (!$style instanceof StringStyles) {
            return $context->functions()->invalidResult(new InvalidArgumentException('StringStyle function requires valid StringStyles enum'));
        }

        return $style->convert($string);
    }
}
