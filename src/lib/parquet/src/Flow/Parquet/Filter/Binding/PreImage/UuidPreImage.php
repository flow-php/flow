<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\PreImage;
use Flow\Parquet\ParquetFile\Data\Converter\UuidConverter;

/**
 * Only the canonical lowercase form: the column decodes to it, so another spelling never equals a value.
 */
final readonly class UuidPreImage implements PreImage
{
    public function __construct(
        private UuidConverter $converter,
    ) {}

    public function equal(bool|DateTimeImmutable|float|int|string $literal): ?EqualRange
    {
        /** @var string $literal LiteralDomain::Uuid holds only lowercase uuid strings */
        return EqualRange::point($this->converter->toParquetType($literal));
    }
}
