<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function mb_strlen;
use function mb_substr;
use function str_repeat;

final class Sanitize extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|string $placeholder,
        private readonly ScalarFunction|int|null $skipCharacters = null,
    ) {}

    public function eval(Row $row, FlowContext $context): ?string
    {
        $val = (new Parameter($this->value))->asString($row, $context);
        $placeholder = (new Parameter($this->placeholder))->asString($row, $context);
        $skipCharacters = (new Parameter($this->skipCharacters))->asInt($row, $context);

        if ($val === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Sanitize function requires non-null value'));
        }

        if ($placeholder === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Sanitize function requires non-null placeholder'));
        }

        $size = mb_strlen($val);

        if ($skipCharacters !== null && $size > $skipCharacters) {
            return mb_substr($val, 0, $skipCharacters) . str_repeat($placeholder, $size - $skipCharacters);
        }

        return str_repeat($placeholder, $size);
    }
}
