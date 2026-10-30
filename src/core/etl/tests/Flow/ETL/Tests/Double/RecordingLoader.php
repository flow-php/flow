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

/**
 * Logs every call in order - `load#<n>(<rows>)`, `load#<n> THROW`, `closure`, `closure THROW`, `discard` - and the
 * error handler of every context it loads under.
 */
final class RecordingLoader implements Closure, Discardable, Loader
{
    /**
     * @var list<string>
     */
    public array $log = [];

    public int $loadsCount = 0;

    public function __construct(
        private readonly ?Throwable $loadFailure = null,
        private readonly int $failingLoad = 1,
        private readonly ?Throwable $closureFailure = null,
    ) {}

    public function closure(): void
    {
        if ($this->closureFailure !== null) {
            $this->log[] = 'closure THROW';

            throw $this->closureFailure;
        }

        $this->log[] = 'closure';
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
        $this->log[] = 'discard';
    }

    public function load(Rows $rows, Backend $backend): void
    {
        $this->loadsCount++;

        if ($this->loadFailure !== null && $this->loadsCount === $this->failingLoad) {
            $this->log[] = 'load#' . $this->loadsCount . ' THROW';

            throw $this->loadFailure;
        }

        $this->log[] = 'load#' . $this->loadsCount . '(' . $rows->count() . ')';
    }
}
