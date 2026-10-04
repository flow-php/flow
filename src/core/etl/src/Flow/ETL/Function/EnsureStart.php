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

final class EnsureStart implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $prefix;

    public function __construct(ScalarFunction|string $value, ScalarFunction|string $prefix)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->prefix = $prefix instanceof ScalarFunction ? $prefix : lit($prefix);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->prefix];
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
        $prefixes = (new Parameter($this->prefix))->asStrings($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $prefix = $prefixes[$i];

                if ($value === null) {
                    throw new InvalidArgumentException('EnsureStart function requires non-null value');
                }

                if ($prefix === null || $prefix === '') {
                    $results[] = $value;

                    continue;
                }

                $results[] = s($value)->ensureStart($prefix)->toString();
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
