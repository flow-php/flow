<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\Statistics;
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
     * @param null|int<1, max> $limit rows in total the plan needs from this read. A hint: the Limit step above the
     *                                source enforces the exact count, so a source that reads more, or ignores it,
     *                                is still correct.
     *
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context, ?int $limit = null): Generator;

    /**
     * Answers before extract() runs, so every batch it yields carries this shape. Takes no
     * FlowContext: a source that needs the pipeline's context to describe itself has not moved
     * the answer to bind time.
     *
     * Called once per RUN - every run plans afresh - so it MUST be idempotent and cheap on repeat: memoise
     * what it sniffs, as CSVExtractor and ArrayExtractor do.
     *
     * @throws SchemaNotDerivableException when the source cannot describe what it will produce
     */
    public function schema(): Schema;

    /**
     * What the source knows about the data before a single row is read: how many rows, how many bytes. A source
     * that knows nothing returns `new Statistics()`. Describes the source with NO pushdown applied - a limit or a
     * partition filter the plan pushed is the plan's to account for.
     *
     * Called AT MOST ONCE per run, and only when something needs the answer - so it must be lazy, never computed in
     * the constructor - and it MUST be idempotent and cheap on repeat: memoise what it reads, the way schema() does.
     *
     * A file source may only spend what the run already spends: it may complete a listing it must produce anyway and
     * read metadata from the files schema() already opens, and it may extrapolate from those. It must not open an
     * extra file or make an extra remote call. A database source may ask the planner for an estimate (EXPLAIN without
     * ANALYZE), never run the query.
     */
    public function statistics(): Statistics;

    /**
     * Declares the shape every yielded batch must carry. A source that describes itself uses this
     * instead of its own description, and values are cast to fit.
     */
    public function withSchema(Schema $schema): static;
}
