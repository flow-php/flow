<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeInterface;
use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->dateTime))->asInstancesOf($rows, $context, DateTimeInterface::class);
        $formats = (new Parameter($this->format))->asStrings($rows, $context);
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

        return (new ResultColumn())->of($this, $results);
    }
}
