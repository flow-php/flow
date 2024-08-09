<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class CallMethod extends ScalarFunctionChain
{
    /**
     * @param object $object
     * @param ScalarFunction|string $method
     * @param array<mixed> $params
     */
    public function __construct(
        private readonly object $object,
        private readonly ScalarFunction|string $method,
        private readonly array $params = []
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $object = (new Parameter($this->object))->asObject($row);
        $method = (new Parameter($this->method))->asString($row);

        if (\is_object($object) && \is_string($method) && \method_exists($object, $method)) {
            return $object->{$method}(...\array_map(
                static fn (mixed $param) : mixed => (new Parameter($param))->eval($row),
                $this->params
            ));
        }

        return null;
    }
}
