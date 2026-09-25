<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeInterface;
use DateTimeZone;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_datetime;

final class ToTimeZone implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;

    /**
     * @var DateTimeType<\DateTimeInterface>
     */
    private readonly DateTimeType $type;

    public function __construct(ScalarFunction|DateTimeInterface $value, DateTimeZone|string $timezone)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->type = type_datetime($timezone);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->type->zone());
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return $this->type;
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        $dateTime = (new Parameter($this->value))->asInstanceOf($row, $context, DateTimeInterface::class);

        if ($dateTime === null) {
            throw new InvalidArgumentException('ToTimeZone function requires non-null values');
        }

        return $this->type->cast($dateTime);
    }
}
