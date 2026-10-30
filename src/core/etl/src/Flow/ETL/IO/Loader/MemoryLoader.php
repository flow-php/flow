<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Loader;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader;
use Flow\ETL\Storage\Memory\Memory;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

final readonly class MemoryLoader implements Loader
{
    public function __construct(
        private Memory $memory,
    ) {}

    public function load(Rows $rows, Backend $backend): void
    {
        // @mago-ignore analysis:less-specific-argument
        $this->memory->save($rows->toArray());
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }
}
