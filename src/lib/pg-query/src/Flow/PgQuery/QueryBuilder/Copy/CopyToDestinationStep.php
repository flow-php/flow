<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Copy;

interface CopyToDestinationStep
{
    public function toFile(string $filename) : CopyToOptionsStep;

    public function toProgram(string $command) : CopyToOptionsStep;

    public function toStdout() : CopyToOptionsStep;
}
