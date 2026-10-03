<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;
use function Symfony\Component\String\s;

final class StringEqualsTo implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $string;

    public function __construct(ScalarFunction|string $value, ScalarFunction|string $string)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->string = $string instanceof ScalarFunction ? $string : lit($string);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->string];
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
        return (new Nullability())->any(type_boolean(), $this->value->returns(), $this->string->returns());
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->value))->asStrings($rows, $context);
        $strings = (new Parameter($this->string))->asStrings($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $string = $strings[$i];

                if ($value === null || $string === null) {
                    $results[] = null;

                    continue;
                }

                $results[] = s($value)->equalsTo($string);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
