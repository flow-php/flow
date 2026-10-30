<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Fixtures\Join;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Join\Condition;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

final readonly class AlwaysMeets implements Condition
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function compare(Rows $left, Rows $right): array
    {
        $met = [];

        for ($i = 0, $count = $left->count(); $i < $count; $i++) {
            $met[] = true;
        }

        return $met;
    }

    public function left(): array
    {
        return [];
    }

    public function right(): array
    {
        return [];
    }
}
