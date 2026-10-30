<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

use function array_map;
use function array_merge;

final class SpyLoader implements Closure, Loader
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public int $closureCount = 0;

    /**
     * @var list<Backend>
     */
    public array $backends = [];

    /**
     * @var list<Rows>
     */
    public array $loadedRows = [];

    public int $loadsCount = 0;

    public function closure(): void
    {
        $this->closureCount++;
    }

    /**
     * @return list<int>
     */
    public function loadedRowCounts(): array
    {
        return array_map(static fn(Rows $rows): int => $rows->count(), $this->loadedRows);
    }

    /**
     * @return array<int, array<array-key, mixed>>
     */
    public function loadedRowsToArray(): array
    {
        return array_merge(...array_map(static fn(Rows $rows): array => $rows->toArray(), $this->loadedRows));
    }

    public function load(Rows $rows, Backend $backend): void
    {
        $this->loadedRows[] = $rows;
        $this->backends[] = $backend;
        $this->loadsCount++;
    }

    public function reset(): void
    {
        $this->closureCount = 0;
        $this->backends = [];
        $this->loadedRows = [];
        $this->loadsCount = 0;
    }
}
