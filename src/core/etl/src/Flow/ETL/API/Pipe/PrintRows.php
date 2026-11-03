<?php

declare(strict_types=1);

namespace Flow\ETL\API\Pipe;

use Flow\ETL\API\DataFrame;
use Flow\ETL\IO\Formatter;
use Flow\ETL\IO\Formatter\AsciiTableFormatter;

final readonly class PrintRows
{
    public function __construct(
        private ?int $limit = 20,
        private int|bool $truncate = 20,
        private Formatter $formatter = new AsciiTableFormatter(),
    ) {}

    public function __invoke(DataFrame $dataFrame): void
    {
        $dataFrame->printRows($this->limit, $this->truncate, $this->formatter);
    }
}
