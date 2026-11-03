<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use NoDiscard;

use function array_values;

final readonly class DuplicateRow implements Transformation
{
    /**
     * @var list<WithColumn>
     */
    private array $columns;

    public function __construct(
        private mixed $condition,
        WithColumn ...$columns,
    ) {
        $this->columns = array_values($columns);
    }

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->duplicateRow($this->condition, ...$this->columns);
    }
}
