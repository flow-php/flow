<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor\Repeatability;
use Flow\ETL\Row\Reference;

use function array_values;
use function count;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function sort;
use function sprintf;

final readonly class DiscoveredPivotValues implements PivotValues
{
    public function __construct(
        private int $maxValues = 10_000,
    ) {
        if ($this->maxValues < 1) {
            throw new InvalidArgumentException('discover_pivot_values() must allow at least one value, given: '
            . $this->maxValues);
        }
    }

    public function resolve(DataFrame $source, Reference $pivot): DeclaredPivotValues
    {
        if (!(new Repeatability())->of($source->extractor())) {
            throw SchemaNotDerivableException::nonRewindable($source->extractor()::class);
        }

        $distinct = [];

        // the frame, not its source: the pivot column may be produced by an earlier step, and an
        // earlier filter decides which values exist at all
        foreach ($source->get() as $batch) {
            foreach ($batch as $row) {
                /** @var mixed $value */
                $value = $row->get($pivot);

                if ($value === null) {
                    continue;
                }

                $value = type_union(type_string(), type_integer())->assert($value);
                $distinct[$value] = $value;

                if (count($distinct) > $this->maxValues) {
                    throw new InvalidArgumentException(sprintf(
                        'discover_pivot_values() found more than %d distinct values in column "%s". '
                        . 'Raise the limit, or declare them with pivot_values(...).',
                        $this->maxValues,
                        $pivot->name(),
                    ));
                }
            }
        }

        if ($distinct === []) {
            throw new InvalidArgumentException(sprintf(
                'discover_pivot_values() found no values in column "%s", so the pivot has no columns to declare.',
                $pivot->name(),
            ));
        }

        $values = array_values($distinct);
        // sorted so the discovered column order is deterministic across runs
        sort($values);

        return new DeclaredPivotValues(...$values);
    }
}
