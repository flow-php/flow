<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function str_pad;

final class StrPad extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|int $length,
        private readonly ScalarFunction|string $padString = ' ',
        private readonly ScalarFunction|int $type = STR_PAD_RIGHT,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $length = (new Parameter($this->length))->asInt($row, $context);
        $padString = (new Parameter($this->padString))->asString($row, $context);
        $type = (new Parameter($this->type))->asInt($row, $context);

        if ($value === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StrPad function requires non-null value'));
        }

        if ($length === null || $padString === null || $type === null) {
            return $context
                ->functions()
                ->invalidResult(
                    new InvalidArgumentException('StrPad function requires non-null length, padString and type'),
                );
        }

        return str_pad($value, $length, $padString, $type);
    }
}
