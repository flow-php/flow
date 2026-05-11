<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Value\Json;

final class JsonDecode extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $value,
        private readonly ScalarFunction|int $flags = JSON_THROW_ON_ERROR,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $value = (new Parameter($this->value))->eval($row, $context);
        $flags = (int) (new Parameter($this->flags))->asInt($row, $context);

        if ($value === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('JsonDecode function requires non-null value'));
        }

        if ($value instanceof Json) {
            return $value->toArray();
        }

        if (\is_array($value)) {
            return $value;
        }

        if (!\is_string($value)) {
            return $context
                ->functions()
                ->invalidResult(
                    new InvalidArgumentException('JsonDecode function requires string, array, or Json value'),
                );
        }

        try {
            return \json_decode($value, true, 512, $flags);
        } catch (\JsonException $e) {
            $context->functions()->invalidResult(new InvalidArgumentException('JsonDecode error: ' . $e->getMessage()));

            return null;
        }
    }
}
