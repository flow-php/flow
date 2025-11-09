<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\{type_instance_of, type_string};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class ToTimeZone extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|\DateTimeInterface $value,
        private readonly ScalarFunction|\DateTimeZone|string $timezone,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : mixed
    {
        $dateTime = (new Parameter($this->value))->asInstanceOf($row, $context, \DateTimeInterface::class);
        $tz = (new Parameter($this->timezone))->as($row, $context, type_string(), type_instance_of(\DateTimeZone::class));

        if ($dateTime === null || $tz === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('ToTimeZone function requires non-null values'));
        }

        $tz = match (\gettype($tz)) {
            'string' => new \DateTimeZone($tz),
            'object' => $tz instanceof \DateTimeZone ? $tz : null,
            default => null,
        };

        if ($tz === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('ToTimeZone function requires valid DateTimeZone'));
        }

        /** @var \DateTime|\DateTimeImmutable $dateTime */
        return $dateTime->setTimezone($tz);
    }
}
