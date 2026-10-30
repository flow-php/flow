<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use ArrayObject;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

final class CallOrderLoader implements Loader
{
    /**
     * @param ArrayObject<int, string> $log shared by every loader whose call order a test compares
     */
    public function __construct(
        private readonly string $name,
        private readonly ArrayObject $log,
    ) {}

    public function load(Rows $rows, Backend $backend): void
    {
        $this->log[] = "{$this->name}:{$rows->count()}";
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
