<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Loader;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

final class ArrayLoader implements Loader
{
    /**
     * @param array<array<mixed>> $array
     */
    public function __construct(
        private array &$array,
    ) {}

    public function load(Rows $rows, Backend $backend): void
    {
        foreach ($rows->toArray() as $row) {
            $this->array[] = $row;
        }
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
