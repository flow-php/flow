<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\PreImage;
use Flow\Parquet\ParquetFile\Data\Converter\UuidConverter;

use function is_string;
use function preg_match;

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
        if (
            !is_string($literal)
            || preg_match('/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/', $literal) !== 1
        ) {
            return null;
        }

        return EqualRange::point($this->converter->toParquetType($literal));
    }
}
