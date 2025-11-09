<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class JsonEncode extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $value,
        private readonly ScalarFunction|int $flags = JSON_THROW_ON_ERROR,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : mixed
    {
        $value = (new Parameter($this->value))->eval($row, $context);
        $flags = (int) (new Parameter($this->flags))->asInt($row, $context);

        try {
            return \json_encode($value, $flags);
        } catch (\JsonException $e) {
            $context->functions()->invalidResult(new InvalidArgumentException('JsonEncode error: ' . $e->getMessage()));

            return null;
        }
    }
}
