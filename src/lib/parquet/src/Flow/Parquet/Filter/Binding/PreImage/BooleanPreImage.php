<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\PreImage;

use function is_bool;

final readonly class BooleanPreImage implements PreImage
{
    public function equal(bool|DateTimeImmutable|float|int|string $literal): ?EqualRange
    {
        return is_bool($literal) ? EqualRange::point($literal) : null;
    }
}
