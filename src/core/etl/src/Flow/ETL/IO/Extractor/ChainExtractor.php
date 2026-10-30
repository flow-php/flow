<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

use function array_reduce;

final class ChainExtractor implements Extractor, Refreshable
{
    private ?Schema $schema = null;

    /**
     * @var array<Extractor>
     */
    private array $extractors;

    public function __construct(Extractor ...$extractors)
    {
        $this->extractors = $extractors;
    }

    public function isRepeatable(): bool
    {
        foreach ($this->extractors as $extractor) {
            if (!$extractor->isRepeatable()) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        // Every child's batches are projected onto the folded shape, so each yields the union
        // rather than only the columns its own source happens to carry. A child that cannot
        // describe itself makes the chain unable to promise one shape, so the fold is allowed to
        // throw - on the first batch, so a chain that yields nothing reads under any declared parent.
        $schema = null;

        foreach ($this->extractors as $extractor) {
            foreach ($extractor->extract($backend, new Criteria()) as $rows) {
                $schema ??= $this->schema();
                $signal = yield $rows->matchTo($schema, $backend);

                if ($signal === Signal::STOP) {
                    return;
                }
            }
        }
    }

    public function refresh(): void
    {
        foreach ($this->extractors as $extractor) {
            if ($extractor instanceof Refreshable) {
                $extractor->refresh();
            }
        }
    }

    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        if ($this->extractors === []) {
            throw InferredSchemaException::nothingToInferFrom('from_all()', 'it chains no extractor');
        }

        $schema = new Schema();

        foreach ($this->extractors as $extractor) {
            $schema = $schema->merge($extractor->schema());
        }

        return $schema;
    }

    public function statistics(): Statistics
    {
        return array_reduce(
            $this->extractors,
            static fn(Statistics $statistics, Extractor $extractor): Statistics => $statistics->merge(
                $extractor->statistics(),
            ),
            new Statistics(rows: Cardinality::exact(0), size: Cardinality::exact(0)),
        );
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
