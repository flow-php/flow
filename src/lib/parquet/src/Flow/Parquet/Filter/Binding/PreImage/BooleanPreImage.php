<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\PreImage;

final readonly class BooleanPreImage implements PreImage
{
    public function equal(bool|DateTimeImmutable|float|int|string $literal): ?EqualRange
    {
        /** @var bool $literal LiteralDomain::Boolean holds only booleans */
        return EqualRange::point($literal);
    }
}
