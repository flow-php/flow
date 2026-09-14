<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor\Scan;
use Flow\ETL\Extractor\Scannable;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function array_values;

/**
 * Takes a pushed Scan but lists no files - Scannable without being a FileExtractor.
 */
final class RecordingScannableExtractor implements Scannable
{
    /**
     * @var list<Scan>
     */
    public array $scans = [];

    /**
     * @var list<Rows>
     */
    private readonly array $batches;

    public function __construct(
        private readonly Schema $schema,
        Rows ...$batches,
    ) {
        $this->batches = array_values($batches);
    }

    public function extract(FlowContext $context, Scan $scan = new Scan()): Generator
    {
        $this->scans[] = $scan;

        foreach ($this->batches as $rows) {
            yield $rows;
        }
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }
}
