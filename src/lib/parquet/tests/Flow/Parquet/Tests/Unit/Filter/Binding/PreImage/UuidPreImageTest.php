<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding\PreImage;

use Flow\Parquet\Filter\Operator;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Context\PhysicalIntervalEdges;
use PHPUnit\Framework\TestCase;

use function hex2bin;

final class UuidPreImageTest extends TestCase
{
    public function test_a_canonical_uuid_string_binds_to_its_16_bytes(): void
    {
        $bytes = hex2bin('f6d6e0e84b7e4b0e8d7aff0a0c9c9a5a');

        static::assertSame(
            [[$bytes, true, $bytes, true]],
            PhysicalIntervalEdges::ofComparison(
                FlatColumn::uuid('u'),
                Operator::Equal,
                'f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a',
            ),
        );
        static::assertNull(PhysicalIntervalEdges::intervals(
            FlatColumn::uuid('u'),
            Operator::Equal,
            'F6D6E0E8-4B7E-4B0E-8D7A-FF0A0C9C9A5A',
        ));
        static::assertNull(PhysicalIntervalEdges::intervals(FlatColumn::uuid('u'), Operator::Equal, 1));
    }
}
