<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeInterface;
use DateTimeZone;
use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $column = (new Parameter($this->value))->column($rows, $context);

        if (type_bare($column->type()) instanceof DateTimeType) {
            for ($i = 0, $count = $column->count(); $column->nullCount() > 0 && $i < $count; $i++) {
                if ($column->isNull($i)) {
                    throw new EvaluationException(
                        $i,
                        new InvalidArgumentException('ToTimeZone function requires non-null values'),
                    );
                }
            }

            return $column->withType($this->type);
        }

        $results = [];
        $i = 0;

        try {
            foreach ((new Parameter($this->value))->asInstancesOf(
                $rows,
                $context,
                DateTimeInterface::class,
            ) as $i => $dateTime) {
                if ($dateTime === null) {
                    throw new InvalidArgumentException('ToTimeZone function requires non-null values');
                }

                $results[] = $this->type->cast($dateTime);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
