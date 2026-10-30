<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use SensitiveParameter;

use function bin2hex;
use function ceil;
use function max;
use function random_bytes;
use function random_int;
use function substr;

final class NativePHPRandomValueGenerator implements Describable, RandomValueGenerator
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    public function describe(): ?Description
    {
        return new Description(self::class, []);
    }

    public function int(int $min, int $max): int
    {
        return random_int($min, $max);
    }

    public function string(int $int): string
    {
        $bytes = (int) ceil($int / 2);
        $bytes >= 1 ?: ($bytes = 1);

        return substr(bin2hex(random_bytes($bytes)), 0, max(0, $int));
    }
}
