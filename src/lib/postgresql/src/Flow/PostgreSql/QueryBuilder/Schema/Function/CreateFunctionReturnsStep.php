<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\QueryBuilder\Schema\DataType;

interface CreateFunctionReturnsStep
{
    public function returns(DataType $type) : CreateFunctionOptionsStep;

    public function returnsSetOf(DataType $type) : CreateFunctionOptionsStep;

    /**
     * @param array<string, DataType> $columns
     */
    public function returnsTable(array $columns) : CreateFunctionOptionsStep;

    public function returnsVoid() : CreateFunctionOptionsStep;
}
