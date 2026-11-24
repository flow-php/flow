<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Schema;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\schema_fingerprint_options;

final class FingerprintOptionsTest extends FlowTestCase
{
    public function test_default_has_no_metadata_keys(): void
    {
        static::assertSame([], schema_fingerprint_options()->metadataKeys);
    }

    public function test_metadata_keys_are_sorted(): void
    {
        static::assertSame(
            ['pii', 'unit'],
            schema_fingerprint_options()->withMetadataKeys('unit', 'pii')->metadataKeys,
        );
    }

    public function test_metadata_keys_are_deduplicated(): void
    {
        static::assertSame(
            ['pii', 'unit'],
            schema_fingerprint_options()->withMetadataKeys('unit', 'pii', 'unit')->metadataKeys,
        );
    }

    public function test_metadata_keys_are_sorted_as_bytes(): void
    {
        static::assertSame(
            ['10', '9', 'B', 'b'],
            schema_fingerprint_options()->withMetadataKeys('b', 'B', '9', '10')->metadataKeys,
        );
    }

    public function test_with_metadata_keys_replaces_previous_keys(): void
    {
        static::assertSame(
            ['b'],
            schema_fingerprint_options()->withMetadataKeys('a')->withMetadataKeys('b')->metadataKeys,
        );
    }
}
