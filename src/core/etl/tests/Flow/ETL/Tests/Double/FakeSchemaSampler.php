<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Inference\SchemaSampler;

final class FakeSchemaSampler implements SchemaSampler
{
    /**
     * @var list<int>
     */
    public array $askedBudgets = [];

    /**
     * @param list<list<RawRowValues>> $units
     */
    public function __construct(
        private readonly array $units,
    ) {}

    /**
     * @return list<list<RawRowValues>>
     */
    public function samples(int $rowBudget): iterable
    {
        $this->askedBudgets[] = $rowBudget;

        return $this->units;
    }
}
