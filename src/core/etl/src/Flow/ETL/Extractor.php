<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor\Signal;
use Generator;

interface Extractor
{
    /**
     * Yields rows in batches. When the source is a BatchableExtractor a batch holds between 0 and that
     * extractor's own batchSize() rows; the size is chosen by the extractor and MAY vary between
     * batches. Consumers must not assume a minimum size, a constant size, or a non-empty batch.
     *
     * yield from is forbidden here: it routes send() into the delegate, so Signal::STOP is never
     * observed by this generator.
     *
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator;

    /**
     * Answers before extract() runs, so every batch it yields carries this shape. Takes no
     * FlowContext: a source that needs the pipeline's context to describe itself has not moved
     * the answer to bind time.
     *
     * @throws SchemaNotDerivableException when the source cannot describe what it will produce
     */
    public function schema(): Schema;

    /**
     * Declares the shape every yielded batch must carry. A source that describes itself uses this
     * instead of its own description, and values are cast to fit.
     */
    public function withSchema(Schema $schema): static;
}
