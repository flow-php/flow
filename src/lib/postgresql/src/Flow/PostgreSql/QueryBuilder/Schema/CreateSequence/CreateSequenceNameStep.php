<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\CreateSequence;

interface CreateSequenceNameStep
{
    public function sequence(string $name, ?string $schema = null): CreateSequenceOptionsStep;
}
