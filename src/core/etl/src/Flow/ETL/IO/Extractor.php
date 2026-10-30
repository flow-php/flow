<?php

declare(strict_types=1);

namespace Flow\ETL\IO;

use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

interface Extractor
{
    /**
     * Yields rows in batches. When the source is a BatchableExtractor a batch holds between 0 and that
     * extractor's own batchSize() rows; the size is chosen by the extractor and MAY vary between
     * batches. Consumers must not assume a minimum size, a constant size, or a non-empty batch.
     *
     * `yield from` is allowed only as the whole body: it routes send() into the delegate, so the delegate sees
     * Signal::STOP and this generator ends with it. A generator that does anything after delegating forwards with
     * foreach + send().
     *
     * $criteria is what the plan pushed into this read. A source returns only rows matching every one of
     * $criteria->conjuncts; only a FileExtractor ever receives any. The limit and the path filter are hints: the plan
     * still enforces the limit, and the predicate of every path filter is also one of the conjuncts or stays in a
     * Filter node, so a source that reads or lists more is still correct. Batches built on another backend than
     * $backend are adopted by the runtime.
     *
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator;

    /**
     * True when extract() can run more than once and yield the same rows each time, so a build-time pass may read
     * the source before the run without consuming it. A one-shot stream returns false; a wrapper answers for what it
     * reads.
     */
    public function isRepeatable(): bool;

    /**
     * Answers before extract() runs, so every batch it yields carries this shape. Takes no
     * FlowContext: a source that needs the pipeline's context to describe itself has not moved
     * the answer to bind time.
     *
     * Called once per RUN - every run plans afresh - so it MUST be idempotent and cheap on repeat: memoise
     * what it sniffs, as CSVExtractor and ArrayExtractor do.
     *
     * @throws InferredSchemaException when no schema was declared and the source has no column to infer one from
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
