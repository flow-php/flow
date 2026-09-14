<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Scan;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Path;
use Generator;

use function array_values;
use function Flow\Filesystem\DSL\path;

/**
 * A push-down source that records every Scan it was run with, so a test can prove what the planner
 * pushed reached the read - and that the instance itself was left alone.
 */
final class RecordingScanExtractor implements FileExtractor
{
    /**
     * @var list<Scan>
     */
    public array $scans = [];

    /**
     * @var list<Rows>
     */
    private readonly array $batches;

    private Schema $partitions;

    public function __construct(
        private readonly Schema $schema,
        Rows ...$batches,
    ) {
        $this->batches = array_values($batches);
        $this->partitions = new Schema();
    }

    public function extract(FlowContext $context, Scan $scan = new Scan()): Generator
    {
        $this->scans[] = $scan;

        foreach ($this->batches as $rows) {
            yield $rows;
        }
    }

    public function partitionSchema(): Schema
    {
        return $this->partitions;
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    public function source(): Path
    {
        return path('/dev/null');
    }

    public function withPartitionSchema(Schema $partitions): self
    {
        $self = new self($this->schema, ...$this->batches);
        $self->partitions = $partitions;

        return $self;
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }
}
