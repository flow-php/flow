<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Row;
use Flow\Types\Type;

use function array_combine;
use function array_keys;
use function array_map;
use function array_values;
use function call_user_func;
use function Flow\ETL\DSL\lit;
use function is_callable;

final class CallUserFunc implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var callable|ScalarFunction
     */
    private $callable;

    /**
     * @param callable|ScalarFunction $callable
     * @param array<mixed> $parameters
     * @param Type<mixed> $returnType
     */
    /**
     * @var array<array-key, ScalarFunction>
     */
    private readonly array $parameters;

    public function __construct(
        ScalarFunction|callable $callable,
        array $parameters,
        private readonly Type $returnType,
    ) {
        $this->callable = $callable;
        $this->parameters = array_map(static fn(mixed $parameter): ScalarFunction => $parameter
            instanceof ScalarFunction
                ? $parameter
                : lit($parameter), $parameters);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return array_values($this->parameters);
    }

    /**
     * String keys in the bag become PHP named arguments at call time, so the key list is
     * carried as a field and restored here (Spark's otherCopyArgs).
     *
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($this->callable, array_combine(array_keys($this->parameters), $children), $this->returnType);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return $this->returnType;
    }

    public function eval(Row $row, FlowContext $context): ScalarResult
    {
        $callable = (new Parameter($this->callable))->eval($row, $context);

        if (!is_callable($callable)) {
            throw new InvalidArgumentException('CallUserFunc requires a valid callable');
        }

        $parameters = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->parameters as $key => $parameter) {
            $parameters[$key] = (new Parameter($parameter))->eval($row, $context);
        }

        // The callable's output may not match the declared type - coerce it before trusting the ScalarResult.
        // @mago-ignore analysis:mixed-assignment
        $result = call_user_func($callable, ...$parameters);

        return new ScalarResult($result === null ? null : $this->returnType->cast($result), $this->returnType);
    }
}
