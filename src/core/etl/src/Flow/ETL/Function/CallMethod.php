<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class CallMethod extends ScalarFunctionChain
{
    /**
     * @var ScalarFunction[]
     */
    private readonly array $params;

    public function __construct(private readonly ScalarFunction $object, private readonly ScalarFunction $method, ScalarFunction ...$params)
    {
        $this->params = $params;
    }

    public function eval(Row $row) : mixed
    {
        /** @var ?object $object */
        $object = $this->object->eval($row);
        /** @var ?string $method */
        $method = $this->method->eval($row);

        if (\is_object($object) && \is_string($method) && \method_exists($object, $method)) {
            return $object->{$method}(...\array_map(
                static fn (ScalarFunction $param) : mixed => $param->eval($row),
                $this->params
            ));
        }

        return null;
    }
}
