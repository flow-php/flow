<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\DropSequence;

interface DropSequenceNameStep
{
    public function sequence(string ...$names) : DropSequenceFinalStep;
}
