<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Generator;

use function Flow\ETL\DSL\array_to_rows;

final class DataFrameExtractor implements Extractor
{
    private ?Schema $schema = null;

    public function __construct(
        private DataFrame $dataFrame,
    ) {}

    /**
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        foreach ($this->dataFrame->get() as $rows) {
            if ($this->schema !== null) {
                $rows = array_to_rows($rows->toArray(), $context->hydrator(), $this->schema);
            }

            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }

    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        throw SchemaNotDerivableException::pipeline(self::class);
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
