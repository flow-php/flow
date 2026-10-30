<?php

declare(strict_types=1);

namespace Flow\ETL\API;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\GroupBy\Pivot;
use Flow\ETL\Operators\GroupBy\PivotValues;
use Flow\ETL\Planning\Logical\Node\BucketingAlgorithmBuilder;
use Flow\ETL\Planning\Logical\Trigger;
use Flow\ETL\Values\Rows;
use Generator;

final readonly class GroupedDataFrame
{
    /**
     * @param DataFrame $input the rows feeding the aggregate without the frame's writes - what pivot discovery reads
     */
    public function __construct(
        private DataFrame $df,
        private DataFrame $input,
        private References $refs,
        private ?BucketingAlgorithmBuilder $algorithm = null,
        private ?Pivot $pivot = null,
    ) {}

    public function aggregate(AggregatingFunction ...$aggregations): DataFrame
    {
        if ($aggregations === []) {
            throw new InvalidArgumentException("Aggregations can't be empty");
        }

        return $this->df->aggregateBy(
            new GroupBy($this->refs, new Aggregators(...$aggregations), $this->pivot),
            $this->algorithm,
        );
    }

    /**
     * @throws SchemaNotDerivableException when the values are read from rows that cannot be read a second time
     */
    public function pivot(Reference $ref, PivotValues $values): self
    {
        // the check runs on the first read: values that never read the rows work on any source
        $batches = (
            /** @return Generator<Rows> */ function (): Generator {
                if (!$this->input->explain(Trigger::rows)->logical->isRepeatable()) {
                    throw SchemaNotDerivableException::nonRewindable();
                }

                yield from $this->input->get();
            }
        )();

        return new self(
            $this->df,
            $this->input,
            $this->refs,
            $this->algorithm,
            new Pivot($ref, $values->resolve($batches, $ref)),
        );
    }
}
