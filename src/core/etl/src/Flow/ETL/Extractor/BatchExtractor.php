<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function count;

final class BatchExtractor implements Extractor, OverridingExtractor
{
    private ?Schema $schema = null;

    /**
     * @param int<1, max> $chunkSize
     */
    public function __construct(
        private Extractor $extractor,
        private int $chunkSize,
    ) {}

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        // pinned from the declaration or the first child batch, then every later batch is matched to
        // it - a buffer spans child batches, so its rows must all answer to one schema before trusted()
        $schema = $this->schema;

        $buffer = [];

        foreach ($this->extractor->extract($context) as $rows) {
            $schema ??= $rows->schema();
            $rows = $rows->matchTo($schema);

            foreach ($rows->all() as $row) {
                $buffer[] = $row;

                if (count($buffer) === $this->chunkSize) {
                    $signal = yield Rows::trusted($schema, $buffer);

                    if ($signal === Signal::STOP) {
                        return;
                    }

                    $buffer = [];
                }
            }
        }

        if ($buffer !== []) {
            yield Rows::trusted($schema ?? $this->schema(), $buffer);
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

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
