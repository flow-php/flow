<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\DataFrame;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class CrossJoinRowsTransformer implements Transformer
{
    private ?Rows $rows = null;

    public function __construct(
        private readonly DataFrame $dataFrame,
        private readonly string $prefix = ''
    ) {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->joinCross($this->rows(), $this->prefix);
    }

    private function rows() : Rows
    {
        if ($this->rows === null) {
            $this->rows = $this->dataFrame->fetch();
        }

        return $this->rows;
    }
}
