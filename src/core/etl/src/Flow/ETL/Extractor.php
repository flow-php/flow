<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Generator;

interface Extractor
{
    /**
     * @return \Generator<Rows>
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
