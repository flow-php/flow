<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Row;
use Flow\Types\Type;

final class CallUserFunc extends ScalarFunctionChain
{
    /**
     * @var callable|ScalarFunction
     */
    private $callable;

    /**
     * @param callable|ScalarFunction $callable
     * @param array<mixed> $parameters
     * @param null|Type<mixed> $returnType
     */
    public function __construct(ScalarFunction|callable $callable, private readonly array $parameters, private readonly ?Type $returnType = null)
    {
        $this->callable = $callable;
    }

    public function eval(Row $row) : mixed
    {
        $callable = (new Parameter($this->callable))->eval($row);

        if (!\is_callable($callable)) {
            return null;
        }

        $parameters = [];

        foreach ($this->parameters as $key => $parameter) {
            $parameters[$key] = (new Parameter($parameter))->eval($row);
        }

        if ($this->returnType) {
            return new ScalarResult(
                \call_user_func($callable, ...$parameters),
                $this->returnType
            );
        }

        return \call_user_func($callable, ...$parameters);
    }
}
