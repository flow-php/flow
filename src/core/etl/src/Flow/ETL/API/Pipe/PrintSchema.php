<?php

declare(strict_types=1);

namespace Flow\ETL\API\Pipe;

use Flow\ETL\API\DataFrame;
use Flow\ETL\IO\Formatter\Row\ASCIISchemaFormatter;
use Flow\ETL\IO\Formatter\Schema\SchemaFormatter;

final readonly class PrintSchema
{
    public function __construct(
        private SchemaFormatter $formatter = new ASCIISchemaFormatter(),
    ) {}

    public function __invoke(DataFrame $dataFrame): void
    {
        $dataFrame->printSchema($this->formatter);
    }
}
