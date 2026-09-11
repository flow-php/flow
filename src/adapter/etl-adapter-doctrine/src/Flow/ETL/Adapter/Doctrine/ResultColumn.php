<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

final readonly class ResultColumn
{
    /**
     * @param null|int|string $native the driver's own type identifier, null when it reports none
     */
    public function __construct(
        public string $name,
        public int|string|null $native = null,
    ) {}
}
