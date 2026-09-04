<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
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

    public function eval(Row $row, FlowContext $context): mixed
    {
        $callable = (new Parameter($this->callable))->eval($row, $context);

        if (!is_callable($callable)) {
            throw new InvalidArgumentException('CallUserFunc requires a valid callable');
        }

        $parameters = [];

        foreach ($this->parameters as $key => $parameter) {
            $parameters[$key] = (new Parameter($parameter))->eval($row, $context);
        }

        // The callable's output may not match the declared type - coerce it before trusting it.
        // @mago-ignore analysis:mixed-assignment
        $result = call_user_func($callable, ...$parameters);

        return $result === null ? null : $this->returnType->cast($result);
    }
}
