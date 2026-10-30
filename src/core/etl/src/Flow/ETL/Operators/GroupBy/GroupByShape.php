<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\GroupBy;

use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Values\Schema;

final readonly class GroupByShape
{
    private function __construct(
        public Schema $input,
        public Aggregators $aggregators,
        public Schema $output,
    ) {}

    /**
     * @throws SchemaDefinitionNotFoundException
     */
    public static function of(GroupBy $groupBy, Schema $input): self
    {
        $aggregators = $groupBy->aggregations()->resolved($input);

        return new self($input, $aggregators, $groupBy->outputSchema($input, $aggregators));
    }
}
