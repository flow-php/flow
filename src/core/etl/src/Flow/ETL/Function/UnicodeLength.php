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
use function Flow\Types\DSL\type_integer;
use function Symfony\Component\String\u;

final class UnicodeLength implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;

    public function __construct(ScalarFunction|string $value)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
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
        return new self($children[0]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_integer();
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->value))->asStrings($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                if ($value === null) {
                    throw new InvalidArgumentException('UnicodeLength function requires non-null value');
                }

                $results[] = u($value)->length();
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
