<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function array_shift;

final class BatchByExtractor implements Extractor, OverridingExtractor, RewindableExtractor
{
    private ?Schema $schema = null;

    /**
     * @param null|int<1, max> $minSize
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private Extractor $extractor,
        private Reference $column,
        private ?int $minSize = null,
    ) {
        // @mago-ignore analysis:invalid-operand,impossible-condition,redundant-comparison,redundant-logical-operation
        if ($this->minSize !== null && $this->minSize <= 0) {
            throw new InvalidArgumentException('Minimum batch size must be greater than 0, given: ' . $this->minSize);
        }
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        // pinned from the declaration or the first child batch, then every later batch is matched to
        // it - a buffer spans child batches, so its rows must all answer to one schema before trusted()
        $schema = $this->schema;

        /** @var list<Rows> $pending */
        $pending = [];
        $pendingCount = 0;
        $currentGroupValue = null;

        foreach ($this->extractor->extract($context) as $rows) {
            $schema ??= $rows->schema();
            $rows = $rows->matchTo($schema);

            if ($rows->count() === 0) {
                continue;
            }

            $column = $rows->column($this->column->base());
            $start = 0;

            for ($i = 0; $i < $rows->count(); $i++) {
                // @mago-ignore analysis:mixed-assignment
                $groupValue = $column->value($i);

                if ($currentGroupValue === null) {
                    // @mago-ignore analysis:mixed-assignment
                    $currentGroupValue = $groupValue;
                } elseif ($currentGroupValue !== $groupValue) {
                    if ($this->minSize === null || ($pendingCount + $i - $start) >= $this->minSize) {
                        $pending[] = $rows->slice($start, $i - $start);
                        $first = array_shift($pending);
                        $signal = yield $first->concat(...$pending);

                        if ($signal === Signal::STOP) {
                            return;
                        }

                        $pending = [];
                        $pendingCount = 0;
                        $start = $i;
                    }

                    // @mago-ignore analysis:mixed-assignment
                    $currentGroupValue = $groupValue;
                }
            }

            $pending[] = $rows->slice($start, $rows->count() - $start);
            $pendingCount += $rows->count() - $start;
        }

        $first = array_shift($pending);

        if ($first !== null && $pendingCount > 0) {
            yield $first->concat(...$pending);
        }
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
