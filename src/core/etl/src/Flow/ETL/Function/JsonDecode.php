<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\{type_array, type_string};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class JsonDecode extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $value,
        private readonly ScalarFunction|int $flags = JSON_THROW_ON_ERROR,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : mixed
    {
        $value = (new Parameter($this->value))->as($row, $context, type_string(), type_array());
        $flags = (int) (new Parameter($this->flags))->asInt($row, $context);

        if ($value === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('JsonDecode function requires non-null value'));
        }

        if (\is_array($value)) {
            return $value;
        }

        try {
            return \json_decode(\is_scalar($value) ? (string) $value : '', true, 512, $flags);
        } catch (\JsonException $e) {
            $context->functions()->invalidResult(new InvalidArgumentException('JsonDecode error: ' . $e->getMessage()));

            return null;
        }
    }
}
