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

use function count;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function is_countable;
use function is_string;
use function Symfony\Component\String\s;

final class Size implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;

    public function __construct(mixed $value)
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
        return type_optional(type_integer());
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->value))->values($rows, $context);
        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($values as $i => $value) {
                if (is_string($value)) {
                    $results[] = s($value)->length();

                    continue;
                }

                if (is_countable($value)) {
                    $results[] = count($value);

                    continue;
                }

                $results[] = null;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
