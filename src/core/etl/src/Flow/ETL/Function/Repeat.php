<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

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
use function Symfony\Component\String\s;

final class Repeat implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $times;

    public function __construct(ScalarFunction|string $value, ScalarFunction|int $times)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->times = $times instanceof ScalarFunction ? $times : lit($times);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->times];
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
        $values = (new Parameter($this->value))->asStrings($rows, $context);
        $timesList = (new Parameter($this->times))->asInts($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $times = $timesList[$i];

                if ($value === null) {
                    throw new InvalidArgumentException('Repeat function requires non-null value');
                }

                if ($times === null || $times <= 0) {
                    throw new InvalidArgumentException('Repeat function requires non-null, positive times');
                }

                $results[] = s($value)->repeat($times)->toString();
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
