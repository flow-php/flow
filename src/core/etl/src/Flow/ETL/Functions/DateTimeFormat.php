<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use DateTimeInterface;
use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\TextValues;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use SensitiveParameter;

use function array_search;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_string;
use function is_string;

final class DateTimeFormat implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $dateTime;
    private readonly ScalarFunction $format;

    public function __construct(ScalarFunction|DateTimeInterface $dateTime, ScalarFunction|string $format)
    {
        $this->dateTime = $dateTime instanceof ScalarFunction ? $dateTime : new Literal($dateTime);
        $this->format = $format instanceof ScalarFunction ? $format : new Literal($format);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['date_time'] ?? null, ScalarFunction::class),
            $rebuild->of($data['format'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'date_time' => $this->dateTime,
            'format' => $this->format,
        ]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        // @mago-ignore analysis:mixed-assignment
        $format = $this->format instanceof Literal ? $this->format->value() : null;

        if (is_string($format)) {
            $column = $this->dateTime->eval($rows, $backend);
            $type = type_bare($column->type());

            if ($type instanceof DateTimeType || $type instanceof DateType) {
                /** @var list<?int> $physicals */
                $physicals = $column->physicals();

                if ($column->nullCount() > 0) {
                    throw EvaluationException::at(
                        (int) array_search(null, $physicals, true),
                        new InvalidArgumentException('DateTimeFormat function requires non-null values'),
                    );
                }

                return (new ResultColumn($backend))->of($this, (new TextValues())->dateTimes(
                    $type,
                    $physicals,
                    $format,
                ));
            }
        }

        $values = (new Parameter($this->dateTime))->asInstancesOf($rows, $backend, DateTimeInterface::class);
        $formats = (new Parameter($this->format))->asStrings($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $format = $formats[$i];

                if ($value === null || $format === null) {
                    throw new InvalidArgumentException('DateTimeFormat function requires non-null values');
                }

                $results[] = $value->format($format);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
