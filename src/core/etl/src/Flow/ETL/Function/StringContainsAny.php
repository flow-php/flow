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
use Flow\Types\Type\Nullability;

use function count;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\s;

final class StringContainsAny implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $needles;

    /**
     * @param array<string>|ScalarFunction $needles
     */
    public function __construct(ScalarFunction|string $value, ScalarFunction|array $needles)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->needles = $needles instanceof ScalarFunction ? $needles : lit($needles);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->needles];
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
        return (new Nullability())->any(type_boolean(), $this->value->returns(), $this->needles->returns());
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->value))->asStrings($rows, $context);
        $needlesList = (new Parameter($this->needles))->asArrays($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $needles = $needlesList[$i];

                if ($value === null || $needles === null) {
                    $results[] = null;

                    continue;
                }

                if (count($needles) === 0) {
                    throw new InvalidArgumentException('StringContainsAny function requires a non-empty needles array');
                }

                $typedNeedles = type_list(type_string())->assert($needles);

                $results[] = s($value)->containsAny($typedNeedles);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
