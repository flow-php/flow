<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Window\WindowFrame;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

final class CountingWindowFrame implements WindowFrame
{
    public int $calls = 0;

    /**
     * @param array{0: int, 1: int} $bounds
     */
    public function __construct(
        private readonly array $bounds = [0, 0],
    ) {}

    public function bounds(int $index, Rows $partition): array
    {
        $this->calls++;

        return $this->bounds;
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
