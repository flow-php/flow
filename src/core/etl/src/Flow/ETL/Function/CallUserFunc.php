<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};
use Flow\ETL\Function\ScalarFunction\ScalarResult;
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

    public function eval(Row $row, FlowContext $context) : mixed
    {
        $callable = (new Parameter($this->callable))->eval($row, $context);

        if (!\is_callable($callable)) {
            return $context->functions()->invalidResult(new InvalidArgumentException('CallUserFunc requires a valid callable'));
        }

        $parameters = [];

        foreach ($this->parameters as $key => $parameter) {
            $parameters[$key] = (new Parameter($parameter))->eval($row, $context);
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
