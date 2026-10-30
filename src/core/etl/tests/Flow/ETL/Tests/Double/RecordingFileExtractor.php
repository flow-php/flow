<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Functions\Evaluation\Filter;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Path;
use Generator;

use function array_values;
use function Flow\Filesystem\DSL\path;

/**
 * A file source that records the criteria of every read, so a test can prove what the planner pushed reached the
 * read - and that the instance itself was left alone. It returns only the rows matching the criteria's filter, past its
 * offset, and prunes with every conjunct or with none.
 */
final class RecordingFileExtractor implements FileExtractor
{
    /**
     * @var list<Criteria>
     */
    public array $criteria = [];

    /**
     * @var list<Rows>
     */
    private readonly array $batches;

    private Schema $partitions;

    private bool $prunes = false;

    public function __construct(
        private readonly Schema $schema,
        Rows ...$batches,
    ) {
        $this->batches = array_values($batches);
        $this->partitions = new Schema();
    }

    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        $this->criteria[] = $criteria;
        $filter = Filter::bind($this->schema, ...$criteria->conjuncts);
        $window = new ReadWindow($criteria->offset);
        $skipped = 0;

        foreach ($this->batches as $rows) {
            $filtered = $filter->filter($rows, $backend);
            $kept = $window->skip($filtered, $skipped);
            $skipped += $filtered->count() - $kept->count();

            yield $kept;
        }
    }

    public function partitionSchema(): Schema
    {
        return $this->partitions;
    }

    public function prunes(ScalarFunction $conjunct): bool
    {
        return $this->prunes;
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    public function refresh(): void {}

    public function source(): Path
    {
        return path('/dev/null');
    }

    public function withPartitionSchema(Schema $partitions): self
    {
        $self = new self($this->schema, ...$this->batches);
        $self->partitions = $partitions;
        $self->prunes = $this->prunes;

        return $self;
    }

    public function withPruning(): self
    {
        $self = new self($this->schema, ...$this->batches);
        $self->partitions = $this->partitions;
        $self->prunes = true;

        return $self;
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }

    public function statistics(): Statistics
    {
        return new Statistics();
    }

    public function isRepeatable(): bool
    {
        return false;
    }
}
