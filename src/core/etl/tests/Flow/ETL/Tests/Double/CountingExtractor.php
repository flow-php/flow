<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function array_values;
use function Flow\ETL\DSL\rows;

/**
 * Counts how many times extract() was pulled, so a test can prove the plan bind read no row, and how
 * many batches it yielded, so a test can prove Signal::STOP reached it. The rows are held in memory and
 * re-sliced at its own batch size, so replaying them yields the same rows every time.
 */
final class CountingExtractor implements BatchableExtractor, Extractor, RewindableExtractor
{
    use Batches;

    public int $batchesYielded = 0;

    public int $extractCalls = 0;

    /**
     * @var list<FlowContext>
     */
    public array $contexts = [];

    private readonly Schema $schema;

    /**
     * @var list<Rows>
     */
    private readonly array $batches;

    public function __construct(Schema $schema, Rows ...$batches)
    {
        $this->schema = $schema;
        $this->batches = array_values($batches);
    }

    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        $this->extractCalls++;
        $this->contexts[] = $context;

        $buffer = rows($this->schema);

        foreach ($this->batches as $rows) {
            for ($i = 0; $i < $rows->count(); $i++) {
                $buffer = $buffer->concat(
                    new AdaptiveBackend(),
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

    public function withSchema(Schema $schema): static
    {
        return $this;
    }

    public function statistics(): Statistics
    {
        return new Statistics();
    }
}
