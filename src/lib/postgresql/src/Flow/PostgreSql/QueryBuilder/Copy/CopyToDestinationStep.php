<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Copy;

interface CopyToDestinationStep
{
    public function columns(string ...$columns) : self;

    public function file(string $filename) : CopyToOptionsStep;

    public function program(string $command) : CopyToOptionsStep;

    public function stdout() : CopyToOptionsStep;
}
