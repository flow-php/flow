<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Functions\Reference\Reference;
use NoDiscard;

use function array_values;

final readonly class Repartition implements Transformation
{
    /**
     * @var list<string|Reference>
     */
    private array $columns;

    public function __construct(
        private string|Reference $column,
        string|Reference ...$columns,
    ) {
        $this->columns = array_values($columns);
    }

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->repartition($this->column, ...$this->columns);
    }
}
