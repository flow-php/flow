<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\GroupBy;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Values\Schema;

final readonly class PivotShape
{
    private function __construct(
        public Schema $input,
        public Pivot $pivot,
        public AggregatingFunction $aggregation,
        public Schema $output,
    ) {}

    /**
     * @throws InvalidArgumentException when the group by does not pivot, or a pivot value collides with a group-by column
     * @throws SchemaDefinitionNotFoundException
     */
    public static function of(GroupBy $groupBy, Schema $input): self
    {
        $pivot = $groupBy->pivotedBy();

        if ($pivot === null) {
            throw new InvalidArgumentException('GroupBy does not pivot, there is no pivot shape to derive');
        }

        $aggregation = $groupBy->aggregations()->resolved($input)->first();

        return new self(
            $input,
            $pivot,
            $aggregation,
            $groupBy->pivotSchema($input, $pivot->values->all(), $aggregation),
        );
    }
}
