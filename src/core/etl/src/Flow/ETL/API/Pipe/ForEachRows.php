<?php

declare(strict_types=1);

namespace Flow\ETL\API\Pipe;

use Closure;
use Flow\ETL\API\DataFrame;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Values\Rows;

final readonly class ForEachRows
{
    /**
     * @var null|Closure(Rows $rows, FlowContext $context) : void
     */
    private ?Closure $callback;

    /**
     * @param null|callable(Rows $rows, FlowContext $context) : void $callback
     */
    public function __construct(?callable $callback = null)
    {
        $this->callback = $callback === null ? null : $callback(...);
    }

    public function __invoke(DataFrame $dataFrame): void
    {
        $dataFrame->forEach($this->callback);
    }
}
