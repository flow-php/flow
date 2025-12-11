<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Copy;

interface CopyFromSourceStep
{
    public function columns(string ...$columns) : self;

    public function file(string $filename) : CopyFromOptionsStep;

    public function program(string $command) : CopyFromOptionsStep;

    public function stdin() : CopyFromOptionsStep;
}
