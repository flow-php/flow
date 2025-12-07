<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Copy;

interface CopyFromSourceStep
{
    public function fromFile(string $filename) : CopyFromOptionsStep;

    public function fromProgram(string $command) : CopyFromOptionsStep;

    public function fromStdin() : CopyFromOptionsStep;
}
