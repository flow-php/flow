<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeInterface;
use DateTimeZone;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;
use function is_string;

final class ToTimeZone extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|DateTimeInterface $value,
        private readonly ScalarFunction|DateTimeZone|string $timezone,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $dateTime = (new Parameter($this->value))->asInstanceOf($row, $context, DateTimeInterface::class);
        $tz = (new Parameter($this->timezone))->as(
            $row,
            $context,
            type_string(),
            type_instance_of(DateTimeZone::class),
        );

        if ($dateTime === null || $tz === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('ToTimeZone function requires non-null values'));
        }

        $tz = is_string($tz) ? new DateTimeZone($tz) : $tz;

        /** @var \DateTime|\DateTimeImmutable $dateTime */
        return $dateTime->setTimezone($tz);
    }
}
