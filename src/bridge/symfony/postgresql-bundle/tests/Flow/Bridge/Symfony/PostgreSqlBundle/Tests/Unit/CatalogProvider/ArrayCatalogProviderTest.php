<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\CatalogProvider;

use Flow\Bridge\Symfony\PostgreSqlBundle\CatalogProvider\ArrayCatalogProvider;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\{Catalog, Column, Extension, Func, FunctionVolatility, Index, MaterializedView, Procedure, Schema, Sequence, Table, View};
use Flow\PostgreSql\Schema\Constraint\{PrimaryKey, UniqueConstraint};
use PHPUnit\Framework\TestCase;

final class ArrayCatalogProviderTest extends TestCase
{
    public function test_creates_catalog_from_array_data() : void
    {
        $catalog = new Catalog([
            new Schema('public', [
                new Table(
                    'public',
                    'users',
                    [
                        new Column('id', ColumnType::bigint(), false),
                        new Column('email', ColumnType::varchar(255), false),
                    ],
                    primaryKey: new PrimaryKey(['id'], 'users_pkey'),
                ),
            ]),
        ]);

        $provider = new ArrayCatalogProvider($catalog->normalize());

        $restored = $provider->get();

        self::assertSame(['public'], $restored->names());
        self::assertTrue($restored->get('public')->hasTable('users'));
        self::assertNotNull($restored->get('public')->table('users')->primaryKey);
        self::assertSame('users_pkey', $restored->get('public')->table('users')->primaryKey->name);
        self::assertCount(2, $restored->get('public')->table('users')->columns);
    }

    public function test_creates_catalog_with_all_schema_objects() : void
    {
        $catalog = new Catalog([
            new Schema(
                'public',
                tables: [
                    new Table(
                        'public',
                        'users',
                        [new Column('id', ColumnType::integer(), false)],
                        primaryKey: new PrimaryKey(['id']),
                        indexes: [new Index('idx_id', ['id'], unique: true)],
                        uniqueConstraints: [new UniqueConstraint(['id'], 'uq_id')],
                    ),
                ],
                sequences: [new Sequence('users_id_seq')],
                views: [new View('active_users', 'SELECT * FROM users')],
                materializedViews: [new MaterializedView('user_stats', 'SELECT count(*) FROM users')],
                functions: [new Func('get_user', 'text', ['integer'], 'sql', 'SELECT 1', volatility: FunctionVolatility::STABLE)],
                procedures: [new Procedure('cleanup', [], 'sql', 'DELETE FROM logs')],
                extensions: [new Extension('uuid-ossp', '1.1')],
            ),
        ]);

        $provider = new ArrayCatalogProvider($catalog->normalize());

        $restored = $provider->get();

        $schema = $restored->get('public');
        self::assertCount(1, $schema->tables);
        self::assertCount(1, $schema->sequences);
        self::assertCount(1, $schema->views);
        self::assertCount(1, $schema->materializedViews);
        self::assertCount(1, $schema->functions);
        self::assertCount(1, $schema->procedures);
        self::assertCount(1, $schema->extensions);
    }

    public function test_creates_empty_catalog() : void
    {
        $provider = new ArrayCatalogProvider(['schemas' => []]);

        self::assertSame([], $provider->get()->all());
    }
}
