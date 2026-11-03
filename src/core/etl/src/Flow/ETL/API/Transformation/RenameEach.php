<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Operators\Transformer\Rename\RenameColumnStrategy;
use NoDiscard;

use function array_values;

final readonly class RenameEach implements Transformation
{
    /**
     * @var list<RenameColumnStrategy>
     */
    private array $strategies;

    public function __construct(RenameColumnStrategy ...$strategies)
    {
        $this->strategies = array_values($strategies);
    }

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->renameEach(...$this->strategies);
    }
}
