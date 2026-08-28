<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeInterface;
use DateTimeZone;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;
use function is_string;

final class ToTimeZone implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $timezone;

    public function __construct(ScalarFunction|DateTimeInterface $value, ScalarFunction|DateTimeZone|string $timezone)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->timezone = $timezone instanceof ScalarFunction ? $timezone : lit($timezone);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->timezone];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_datetime();
    }

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
            throw new InvalidArgumentException('ToTimeZone function requires non-null values');
        }

        $tz = is_string($tz) ? new DateTimeZone($tz) : $tz;

        /** @var \DateTime|\DateTimeImmutable $dateTime */
        return $dateTime->setTimezone($tz);
    }
}
