<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\API\Branch;
use Flow\ETL\API\DataFrame;
use Flow\ETL\IO\Loader;

/**
 * Writes the frame it is given into every write, so the writes share that frame's nodes.
 */
final readonly class FanOutBranch implements Branch
{
    /**
     * @var list<Loader|Branch>
     */
    private array $writes;

    public function __construct(Loader|Branch ...$writes)
    {
        $this->writes = array_values($writes);
    }

    public function write(DataFrame $prefix): void
    {
        foreach ($this->writes as $write) {
            $prefix->write($write);
        }
    }
}
