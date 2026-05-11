<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\String\StringStyles;

use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_string;

final class StringStyle extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $string,
        private readonly ScalarFunction|string|StringStyles $style,
    ) {}

    public function eval(Row $row, FlowContext $context): ?string
    {
        $string = (new Parameter($this->string))->asString($row, $context);
        $style = (new Parameter($this->style))->as($row, $context, type_string(), type_enum(StringStyles::class));

        if ($string === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StringStyle function requires non-null value'));
        }

        if ($style === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StringStyle function requires non-null style'));
        }

        if (is_string($style)) {
            $style = StringStyles::fromString($style);
        }

        if (!$style instanceof StringStyles) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StringStyle function requires valid StringStyles enum'));
        }

        return $style->convert($string);
    }
}
