<?php

declare(strict_types=1);

namespace Flow\CLI\Loader;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Symfony\Component\Console\Helper\ProgressBar;

final readonly class ProgressBarLoader implements Loader
{
    public function __construct(
        private ProgressBar $progressBar,
    ) {}

    public function load(Rows $rows, FlowContext $context): void
    {
        $this->progressBar->advance($rows->count());
    }
}
