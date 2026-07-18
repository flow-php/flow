<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Generator;

use function Flow\ETL\DSL\array_to_rows;

final class ArrayExtractor implements Extractor
{
    private ?Schema $schema = null;

    /**
     * @param iterable<array<mixed>> $dataset
     */
    public function __construct(
        private readonly iterable $dataset,
    ) {}

    /**
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        foreach ($this->dataset as $row) {
            $signal = yield array_to_rows([$row], $context->hydrator(), [], $this->schema);

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }

    public function withSchema(Schema $schema): self
    {
        $this->schema = $schema;

        return $this;
    }
}
