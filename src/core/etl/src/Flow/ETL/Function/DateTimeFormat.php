<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class DateTimeFormat extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|\DateTimeInterface $dateTime,
        private readonly ScalarFunction|string $format,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : mixed
    {
        $value = (new Parameter($this->dateTime))->asInstanceOf($row, $context, \DateTimeInterface::class);
        $format = (new Parameter($this->format))->asString($row, $context);

        if ($value === null || $format === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('DateTimeFormat function requires non-null values'));
        }

        return $value->format($format);
    }
}
