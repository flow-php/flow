<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Flow\Types\DSL\type_integer;
use function Symfony\Component\String\s;

final class Wordwrap extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|int $width,
        private readonly ScalarFunction|string $break = "\n",
        private readonly ScalarFunction|bool $cut = false,
    ) {}

    public function eval(Row $row, FlowContext $context): ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $width = type_integer()->assert((new Parameter($this->width))->as($row, $context, type_integer()));
        $break = (new Parameter($this->break))->asString($row, $context);
        $cut = (new Parameter($this->cut))->asBoolean($row, $context);

        if ($value === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Wordwrap function requires non-null value'));
        }

        if ($width <= 0) {
            return $value;
        }

        if ($break === null) {
            $break = "\n";
        }

        return s($value)->wordwrap($width, $break, $cut)->toString();
    }
}
