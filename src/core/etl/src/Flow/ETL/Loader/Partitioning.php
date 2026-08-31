<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;

final readonly class Partitioning
{
    private function __construct(
        public References $by,
        public bool $writeColumns = false,
    ) {}

    public static function by(string|Reference $entry, string|Reference ...$entries): self
    {
        return new self(References::init($entry, ...$entries));
    }

    public static function none(): self
    {
        return new self(References::init());
    }

    /**
     * Keep the partition columns in the file body as well as in the path. Off by default.
     */
    public function writeColumns(bool $writeColumns = true): self
    {
        if (!$this->by->count()) {
            throw new InvalidArgumentException('writeColumns requires at least one partition column');
        }

        return new self($this->by, $writeColumns);
    }
}
