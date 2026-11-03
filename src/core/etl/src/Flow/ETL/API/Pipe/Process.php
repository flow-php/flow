<?php

declare(strict_types=1);

namespace Flow\ETL\API\Pipe;

use Flow\ETL\API\DataFrame;
use Flow\ETL\Flow;
use Flow\ETL\Values\Rows;
use NoDiscard;

use function array_values;

final readonly class Process
{
    /**
     * @var list<Rows>
     */
    private array $rows;

    public function __construct(Rows ...$rows)
    {
        $this->rows = array_values($rows);
    }

    #[NoDiscard]
    public function __invoke(Flow $flow): DataFrame
    {
        return $flow->process(...$this->rows);
    }
}
