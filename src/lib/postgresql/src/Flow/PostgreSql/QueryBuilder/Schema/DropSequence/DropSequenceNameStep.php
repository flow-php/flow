<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\DropSequence;

interface DropSequenceNameStep
{
    public function sequence(string ...$names): DropSequenceFinalStep;
}
