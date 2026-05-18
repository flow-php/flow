<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Trim\Type;
use Flow\ETL\Row;

use function ltrim;
use function rtrim;
use function trim;

final class Trim extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|Type $type = Type::BOTH,
        private readonly ScalarFunction|string $characters = " \t\n\r\0\x0B",
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $type = (new Parameter($this->type))->asEnum($row, $context, Type::class);
        $characters = (new Parameter($this->characters))->asString($row, $context);

        if ($value === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Trim function requires non-null value'));
        }

        if ($type === null || $characters === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Trim function requires non-null type and characters'));
        }

        return match ($type) {
            Type::LEFT => ltrim($value, $characters),
            Type::RIGHT => rtrim($value, $characters),
            Type::BOTH => trim($value, $characters),
        };
    }
}
