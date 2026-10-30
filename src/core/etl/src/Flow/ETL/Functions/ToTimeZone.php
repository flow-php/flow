<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use DateTimeInterface;
use DateTimeZone;
use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use SensitiveParameter;

use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_string;

final class ToTimeZone implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;

    private readonly DateTimeType $type;

    public function __construct(ScalarFunction|DateTimeInterface $value, DateTimeZone|string $timezone)
    {
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->type = type_datetime($timezone);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            type_string()->assert($data['timezone'] ?? null),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'timezone' => $this->type->zone()->getName(),
        ]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $column = $this->value->eval($rows, $backend);

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
                $backend,
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

        return (new ResultColumn($backend))->of($this, $results);
    }
}
