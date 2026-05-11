<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;

interface CreateFunctionReturnsStep
{
    public function returns(ColumnType $type): CreateFunctionOptionsStep;

    public function returnsSetOf(ColumnType $type): CreateFunctionOptionsStep;

    /**
     * @param array<string, ColumnType> $columns
     */
    public function returnsTable(array $columns): CreateFunctionOptionsStep;

    public function returnsVoid(): CreateFunctionOptionsStep;
}
