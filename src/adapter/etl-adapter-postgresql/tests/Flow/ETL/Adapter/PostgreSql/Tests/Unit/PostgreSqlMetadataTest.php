<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\PostgreSqlMetadata;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\PostgreSql\Schema\IdentityGeneration;
use PHPUnit\Framework\TestCase;

final class PostgreSqlMetadataTest extends TestCase
{
    public function test_default_factory(): void
    {
        $metadata = PostgreSqlMetadata::default('now()');

        static::assertTrue($metadata->has(PostgreSqlMetadata::DEFAULT->value));
        static::assertSame('now()', $metadata->get(PostgreSqlMetadata::DEFAULT->value));
    }

    public function test_generated_factory(): void
    {
        $metadata = PostgreSqlMetadata::generated('price * quantity');

        static::assertTrue($metadata->has(PostgreSqlMetadata::GENERATED->value));
        static::assertSame('price * quantity', $metadata->get(PostgreSqlMetadata::GENERATED->value));
    }

    public function test_identity_factory_defaults_to_always(): void
    {
        $metadata = PostgreSqlMetadata::identity();

        static::assertTrue($metadata->has(PostgreSqlMetadata::IDENTITY->value));
        static::assertSame(IdentityGeneration::ALWAYS->value, $metadata->get(PostgreSqlMetadata::IDENTITY->value));
    }

    public function test_identity_factory_with_by_default(): void
    {
        $metadata = PostgreSqlMetadata::identity(IdentityGeneration::BY_DEFAULT);

        static::assertSame(IdentityGeneration::BY_DEFAULT->value, $metadata->get(PostgreSqlMetadata::IDENTITY->value));
    }

    public function test_index_factory(): void
    {
        $metadata = PostgreSqlMetadata::index('idx_name');

        static::assertTrue($metadata->has(PostgreSqlMetadata::INDEX->value . ':idx_name'));
        static::assertSame(PHP_INT_MAX, $metadata->get(PostgreSqlMetadata::INDEX->value . ':idx_name'));
    }

    public function test_index_factory_rejects_colon_in_name(): void
    {
        $this->expectException(InvalidArgumentException::class);

        PostgreSqlMetadata::index('bad:name');
    }

    public function test_index_factory_with_position(): void
    {
        $metadata = PostgreSqlMetadata::index('idx_name', 2);

        static::assertSame(2, $metadata->get(PostgreSqlMetadata::INDEX->value . ':idx_name'));
    }

    public function test_index_unique_factory(): void
    {
        $metadata = PostgreSqlMetadata::indexUnique('uq_email');

        static::assertTrue($metadata->has(PostgreSqlMetadata::INDEX_UNIQUE->value . ':uq_email'));
        static::assertSame(PHP_INT_MAX, $metadata->get(PostgreSqlMetadata::INDEX_UNIQUE->value . ':uq_email'));
    }

    public function test_index_unique_factory_rejects_colon_in_name(): void
    {
        $this->expectException(InvalidArgumentException::class);

        PostgreSqlMetadata::indexUnique('bad:name');
    }

    public function test_index_unique_factory_with_position(): void
    {
        $metadata = PostgreSqlMetadata::indexUnique('uq_email', 1);

        static::assertSame(1, $metadata->get(PostgreSqlMetadata::INDEX_UNIQUE->value . ':uq_email'));
    }

    public function test_merge_keeps_both_index_keys(): void
    {
        $metadata = PostgreSqlMetadata::index('idx_a', 1)->merge(PostgreSqlMetadata::index('idx_b', 2));

        static::assertSame(1, $metadata->get(PostgreSqlMetadata::INDEX->value . ':idx_a'));
        static::assertSame(2, $metadata->get(PostgreSqlMetadata::INDEX->value . ':idx_b'));
    }

    public function test_length_factory(): void
    {
        $metadata = PostgreSqlMetadata::length(255);

        static::assertTrue($metadata->has(PostgreSqlMetadata::LENGTH->value));
        static::assertSame(255, $metadata->get(PostgreSqlMetadata::LENGTH->value));
    }

    public function test_precision_factory(): void
    {
        $metadata = PostgreSqlMetadata::precision(10);

        static::assertTrue($metadata->has(PostgreSqlMetadata::PRECISION->value));
        static::assertSame(10, $metadata->get(PostgreSqlMetadata::PRECISION->value));
    }

    public function test_primary_key_factory_defaults_to_empty_name(): void
    {
        $metadata = PostgreSqlMetadata::primaryKey();

        static::assertTrue($metadata->has(PostgreSqlMetadata::PRIMARY_KEY->value));
        static::assertSame('', $metadata->get(PostgreSqlMetadata::PRIMARY_KEY->value));
    }

    public function test_primary_key_factory_with_name(): void
    {
        $metadata = PostgreSqlMetadata::primaryKey('pk_users');

        static::assertSame('pk_users', $metadata->get(PostgreSqlMetadata::PRIMARY_KEY->value));
    }

    public function test_scale_factory(): void
    {
        $metadata = PostgreSqlMetadata::scale(2);

        static::assertTrue($metadata->has(PostgreSqlMetadata::SCALE->value));
        static::assertSame(2, $metadata->get(PostgreSqlMetadata::SCALE->value));
    }

    public function test_type_factory(): void
    {
        $metadata = PostgreSqlMetadata::type('jsonb');

        static::assertTrue($metadata->has(PostgreSqlMetadata::TYPE->value));
        static::assertSame('jsonb', $metadata->get(PostgreSqlMetadata::TYPE->value));
    }
}
