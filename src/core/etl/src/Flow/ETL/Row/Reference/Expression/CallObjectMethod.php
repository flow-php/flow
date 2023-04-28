<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class CallObjectMethod implements Expression
{
    /**
     * @var Expression[]
     */
    private array $params;

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

        if (null === $object || null === $method || !\method_exists($object, $method)) {
            return null;
        }

        return $object->{$method}(...\array_map(
            static fn (Expression $param) : mixed => $param->eval($row),
            $this->params
        ));
    }
}
