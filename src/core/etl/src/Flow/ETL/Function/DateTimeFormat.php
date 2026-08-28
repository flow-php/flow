<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeInterface;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_string;

final class DateTimeFormat implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $dateTime;
    private readonly ScalarFunction $format;

    public function __construct(ScalarFunction|DateTimeInterface $dateTime, ScalarFunction|string $format)
    {
        $this->dateTime = $dateTime instanceof ScalarFunction ? $dateTime : lit($dateTime);
        $this->format = $format instanceof ScalarFunction ? $format : lit($format);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->dateTime, $this->format];
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
        return type_string();
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        $value = (new Parameter($this->dateTime))->asInstanceOf($row, $context, DateTimeInterface::class);
        $format = (new Parameter($this->format))->asString($row, $context);

        if ($value === null || $format === null) {
            throw new InvalidArgumentException('DateTimeFormat function requires non-null values');
        }

        return $value->format($format);
    }
}
