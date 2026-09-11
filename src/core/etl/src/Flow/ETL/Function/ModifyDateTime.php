<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_datetime;

final class ModifyDateTime implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $reference;
    private readonly ScalarFunction $modifier;

    public function __construct(mixed $reference, string|ScalarFunction $modifier)
    {
        $this->reference = $reference instanceof ScalarFunction ? $reference : lit($reference);
        $this->modifier = $modifier instanceof ScalarFunction ? $modifier : lit($modifier);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->reference, $this->modifier];
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
        $value = (new Parameter($this->reference))->asInstanceOf($row, $context, DateTimeInterface::class);
        $modifier = (new Parameter($this->modifier))->asString($row, $context);

        if ($modifier === null || $value === null) {
            throw new InvalidArgumentException('ModifyDateTime function requires non-null values');
        }

        if (!$value instanceof DateTime && !$value instanceof DateTimeImmutable) {
            throw new InvalidArgumentException('ModifyDateTime function requires DateTime or DateTimeImmutable object');
        }

        return $value->modify($modifier);
    }
}
