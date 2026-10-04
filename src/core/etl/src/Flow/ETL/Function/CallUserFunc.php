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

use function array_combine;
use function array_keys;
use function array_map;
use function array_shift;
use function array_values;
use function call_user_func;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_optional;
use function is_callable;

final class CallUserFunc implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var array<array-key, ScalarFunction>
     */
    private readonly array $parameters;

    /**
     * @param array<mixed> $parameters
     */
    public function __construct(
        private readonly ScalarFunction $callable,
        private readonly Type $returnType,
        array $parameters,
    ) {
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
        return [$this->callable, ...array_values($this->parameters)];
    }

    /**
     * A user callable may keep state or read the outside world, so it is never assumed to answer the same twice.
     */
    public function deterministic(): bool
    {
        return false;
    }

    /**
     * The callable leads the child list; string keys in the parameter bag become PHP named
     * arguments at call time, so the key list is carried as a field and restored here
     *
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        $callable = array_shift($children);

        if (!$callable instanceof ScalarFunction) {
            throw new InvalidArgumentException('CallUserFunc requires a ScalarFunction as its first child');
        }

        return new self($callable, $this->returnType, array_combine(array_keys($this->parameters), $children));
    }

    /**
     * An opaque callable can always answer null, so the declared type has to admit it
     *
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional($this->returnType);
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $callables = (new Parameter($this->callable))->values($rows, $context);
        $arguments = array_map(static fn(ScalarFunction $parameter): array => (new Parameter($parameter))->values(
            $rows,
            $context,
        ), $this->parameters);
        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($callables as $i => $callable) {
                if (!is_callable($callable)) {
                    throw new InvalidArgumentException('CallUserFunc requires a valid callable');
                }

                $parameters = [];

                foreach ($arguments as $key => $argument) {
                    $parameters[$key] = $argument[$i];
                }

                // The callable's output may not match the declared type - coerce it before trusting it.
                // @mago-ignore analysis:mixed-assignment
                $result = call_user_func($callable, ...$parameters);

                $results[] = $result === null ? null : $this->returnType->cast($result);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
