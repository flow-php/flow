<?php

declare(strict_types=1);

namespace Flow\ETL\Transformation;

use function Flow\ETL\DSL\lit;
use Flow\ETL\{DataFrame, Transformation};

/**
 * Mask columns in DataFrame by replacing their values with a mask.
 * If column does not exist in DataFrame, it will be added with a mask value.
 */
final readonly class MaskColumns implements Transformation
{
    public function __construct(private array $columns = [], private string $mask = '******')
    {
    }

    public function transform(DataFrame $dataFrame) : DataFrame
    {
        foreach ($this->columns as $column) {
            $dataFrame->withEntry($column, lit($this->mask));
        }

        return $dataFrame;
    }
}
