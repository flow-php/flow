<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\GroupBy;
use Flow\ETL\Row\References;
use Flow\ETL\Schema;

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
            (new PivotSchema())->of(
                $input,
                References::init(...$groupBy->references()),
                $pivot->values->all(),
                $aggregation,
            ),
        );
    }
}
