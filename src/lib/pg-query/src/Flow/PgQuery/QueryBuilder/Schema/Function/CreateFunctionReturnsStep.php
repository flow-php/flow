<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Function;

interface CreateFunctionReturnsStep
{
    public function returns(string $type) : CreateFunctionOptionsStep;

    public function returnsSetOf(string $type) : CreateFunctionOptionsStep;

    /**
     * @param array<string, string> $columns
     */
    public function returnsTable(array $columns) : CreateFunctionOptionsStep;

    public function returnsVoid() : CreateFunctionOptionsStep;
}
