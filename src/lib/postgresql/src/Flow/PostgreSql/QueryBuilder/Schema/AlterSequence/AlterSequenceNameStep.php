<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterSequence;

interface AlterSequenceNameStep
{
    public function sequence(string $name, ?string $schema = null) : AlterSequenceOptionsStep;
}
