<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class CallMethod implements Expression
{
    /**
     * @var Expression[]
     */
    private readonly array $params;

    public function __construct(private readonly Expression $object, private readonly Expression $method, Expression ...$params)
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
                static fn (Expression $param) : mixed => $param->eval($row),
                $this->params
            ));
        }

        return null;
    }
}
