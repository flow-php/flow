<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Throwable;

final readonly class ThrowingTransformer implements Transformer
{
    public function __construct(
        private Throwable $throwable,
    ) {}

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        throw $this->throwable;
    }
}
