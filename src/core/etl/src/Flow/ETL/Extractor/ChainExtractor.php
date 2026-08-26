<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

final class ChainExtractor implements Extractor, OverridingExtractor
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

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        // Every child is told the folded shape, so each yields the union rather than only the
        // columns its own source happens to carry. A child that cannot describe itself makes the
        // chain unable to promise one shape, so the fold is allowed to throw.
        $schema = $this->schema();

        foreach ($this->extractors as $extractor) {
            $extractor->withSchema($schema);

            foreach ($extractor->extract($context) as $rows) {
                $signal = yield $rows;

                if ($signal === Signal::STOP) {
                    return;
                }
            }
        }
    }

    public function extractors(): array
    {
        return $this->extractors;
    }

    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        $schema = new Schema();

        foreach ($this->extractors as $extractor) {
            $schema = $schema->merge($extractor->schema());
        }

        return $schema;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
