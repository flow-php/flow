<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\IO\Loader\Discardable;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use SensitiveParameter;
use Throwable;

final class ClosureThrowingLoader implements Closure, Discardable, Loader
{
    public int $discarded = 0;

    public int $loadsCount = 0;

    public function __construct(
        private readonly Throwable $throwable,
        private readonly ?Throwable $discardFailure = null,
    ) {}

    public function closure(): void
    {
        throw $this->throwable;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function discard(): void
    {
        $this->discarded++;

        if ($this->discardFailure !== null) {
            throw $this->discardFailure;
        }
    }

    public function load(Rows $rows, Backend $backend): void
    {
        $this->loadsCount++;
    }
}
