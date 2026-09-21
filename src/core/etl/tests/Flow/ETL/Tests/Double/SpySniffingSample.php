<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use ArrayIterator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Inference\ColumnTypes;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SniffsColumnTypes;
use Flow\Types\Type\TypeNarrower;
use IteratorAggregate;
use RuntimeException;

/**
 * A sample that sniffs itself: records what SchemaInferrer hands it and answers with a canned fold. Iterating it
 * fails - infer() must never fall back to row-by-row observe() for a unit that sniffs itself.
 *
 * @implements IteratorAggregate<int, RawRowValues>
 */
final class SpySniffingSample implements IteratorAggregate, SniffsColumnTypes
{
    /**
     * @var list<array{list<string>, int}>
     */
    public array $sniffed = [];

    public function __construct(
        private readonly ColumnTypes $fold,
    ) {}

    /**
     * @return ArrayIterator<int, RawRowValues>
     */
    public function getIterator(): ArrayIterator
    {
        throw new RuntimeException('a sample that sniffs itself is never iterated');
    }

    public function sniffColumnTypes(
        array $names,
        int $rowBudget,
        SchemaInference $inference,
        TypeNarrower $typer,
    ): ColumnTypes {
        $this->sniffed[] = [$names, $rowBudget];

        return $this->fold;
    }
}
