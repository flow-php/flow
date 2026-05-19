<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Value\Json;
use JsonException;

use function is_array;
use function is_object;
use function json_encode;

final class JsonEncode extends ScalarFunctionChain
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
            return null;
        }

        try {
            $encoded = json_encode($value, $flags);

            if ($encoded === false) {
                return $context
                    ->functions()
                    ->invalidResult(new InvalidArgumentException('JsonEncode error: json_encode returned false'));
            }

            if (is_array($value) || is_object($value)) {
                return new Json($encoded);
            }

            return $encoded;
        } catch (JsonException $e) {
            $context->functions()->invalidResult(new InvalidArgumentException('JsonEncode error: ' . $e->getMessage()));

            return null;
        }
    }
}
