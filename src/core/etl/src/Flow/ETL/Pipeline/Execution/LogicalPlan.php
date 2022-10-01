<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\Execution;

use Flow\ETL\Extractor;
use Flow\ETL\Pipeline\Execution\Plan\FilesystemOperations;
use Flow\ETL\Pipeline\Pipes;

final class LogicalPlan
{
    public function __construct(public readonly Extractor $extractor, public readonly Pipes $pipes)
    {
    }

    public function filesystemOperations() : FilesystemOperations
    {
        return new FilesystemOperations(
            $this->extractor,
            $this->pipes->loaders()
        );
    }
}
