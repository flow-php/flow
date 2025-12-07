<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Index\CreateIndex;

interface CreateIndexOnStep
{
    public function concurrently() : self;

    public function ifNotExists() : self;

    public function on(string $table, ?string $schema = null) : CreateIndexColumnsStep;

    public function onOnly(string $table, ?string $schema = null) : CreateIndexColumnsStep;

    public function unique() : self;
}
