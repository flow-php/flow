<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Transformation;

use function Flow\ETL\DSL\lit;
use Flow\ETL\{DataFrame, Transformation};

final class MaskColumns implements Transformation
{
    public function __construct(private readonly array $columns = [], private readonly string $mask = '******')
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
