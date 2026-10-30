<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use SensitiveParameter;

/**
 * A Describable that never crosses: describe() is null.
 */
final readonly class NotDescribed implements Describable
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw new InvalidArgumentException('NotDescribed is never described');
    }

    public function describe(): ?Description
    {
        return null;
    }
}
