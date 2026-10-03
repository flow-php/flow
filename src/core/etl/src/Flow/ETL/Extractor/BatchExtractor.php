<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

final class BatchExtractor implements BatchableExtractor, Extractor, OverridingExtractor, RewindableExtractor
{
    use Batches;

    private ?Schema $schema = null;

    public function __construct(
        private Extractor $extractor,
        int $batchSize,
    ) {
        $this->withBatchSize($batchSize);
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        // pinned from the declaration or the first child batch, then every later batch is matched to it - a window
        // spans child batches, so they must all answer to one schema before they are concatenated
        $schema = $this->schema;

        /** @var list<Rows> $pending */
        $pending = [];
        $pendingCount = 0;

        foreach ($this->extractor->extract($context) as $rows) {
            $schema ??= $rows->schema();
            $pending[] = $rows->matchTo($schema, $context->backend());
            $pendingCount += $rows->count();

            while ($pendingCount >= $this->batchSize) {
                // one variadic concat per window copies each row once; a pairwise concat per child batch would
                // re-copy everything pending
                $window = Rows::empty($schema, $context->backend())->concat($context->backend(), ...$pending);
                $signal = yield $window->slice(0, $this->batchSize);

                if ($signal === Signal::STOP) {
                    return;
                }

                $pending = $window->count() > $this->batchSize
                    ? [$window->slice($this->batchSize, $window->count() - $this->batchSize)]
                    : [];
                $pendingCount -= $this->batchSize;
            }
        }

        if ($pendingCount > 0) {
            yield Rows::empty($schema ?? $this->schema(), $context->backend())->concat(
                $context->backend(),
                ...$pending,
            );
        }
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    public function extractors(): array
    {
        return [$this->extractor];
    }

    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        return $this->extractor->schema();
    }

    public function statistics(): Statistics
    {
        return $this->extractor->statistics();
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
