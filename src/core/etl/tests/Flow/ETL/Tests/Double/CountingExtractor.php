<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\BatchableExtractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

use function array_values;
use function Flow\ETL\DSL\rows;

/**
 * Counts how many times extract() was pulled, so a test can prove the plan bind read no row, and how
 * many batches it yielded, so a test can prove Signal::STOP reached it. The rows are held in memory and
 * re-sliced at its own batch size, so replaying them yields the same rows every time.
 */
final class CountingExtractor implements BatchableExtractor, Extractor
{
    private BatchSize $batchSize;

    public int $batchesYielded = 0;

    public int $extractCalls = 0;

    /**
     * @var list<Backend>
     */
    public array $backends = [];

    private readonly Schema $schema;

    /**
     * @var list<Rows>
     */
    private readonly array $batches;

    public function __construct(Schema $schema, Rows ...$batches)
    {
        $this->schema = $schema;
        $this->batches = array_values($batches);
        $this->batchSize = new BatchSize();
    }

    /**
     * @return int<1, max>
     */
    public function batchSize(): int
    {
        return $this->batchSize->size;
    }

    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        $this->extractCalls++;
        $this->backends[] = $backend;

        $buffer = rows($this->schema);

        foreach ($this->batches as $rows) {
            for ($i = 0; $i < $rows->count(); $i++) {
                $buffer = Rows::concat(
                    $buffer->schema(),
                    new AdaptiveBackend(),
                    $buffer,
                    $rows->slice($i, 1)->matchTo($this->schema, new AdaptiveBackend()),
                );

                if ($buffer->count() === $this->batchSize()) {
                    $this->batchesYielded++;

                    $signal = yield $buffer;

                    if ($signal === Signal::STOP) {
                        return;
                    }

                    $buffer = rows($this->schema);
                }
            }
        }

        if (!$buffer->isEmpty()) {
            $this->batchesYielded++;

            yield $buffer;
        }
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    public function withBatchSize(int $batchSize): static
    {
        $this->batchSize = new BatchSize($batchSize);

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }

    public function statistics(): Statistics
    {
        return new Statistics();
    }
}
