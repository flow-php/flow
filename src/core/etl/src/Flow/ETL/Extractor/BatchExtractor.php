<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

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
        if ($this->schema !== null) {
            $this->extractor->withSchema($this->schema);
        }

        $chunk = null;
        $chunkSize = 0;

        foreach ($this->extractor->extract($context) as $rows) {
            $chunk ??= new Rows($rows->schema());

            foreach ($rows->all() as $row) {
                $chunk = $chunk->add($row);
                $chunkSize++;

                if ($chunkSize === $this->chunkSize) {
                    $signal = yield $chunk;

                    if ($signal === Signal::STOP) {
                        return;
                    }
                    $chunkSize = 0;
                    $chunk = new Rows($rows->schema());
                }

                if ($chunkSize > $this->chunkSize) {
                    $signal = yield $chunk->dropRight($chunk->count() - $this->chunkSize);

                    if ($signal === Signal::STOP) {
                        return;
                    }
                    $chunk = $chunk->takeRight($chunk->count() - $this->chunkSize);
                    $chunkSize = $chunk->count();
                }
            }
        }

        if ($chunkSize && $chunk !== null) {
            yield $chunk;
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
