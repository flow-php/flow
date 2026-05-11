<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\CatalogProvider;

use Flow\Bridge\Symfony\PostgreSqlBundle\CatalogProvider\ArrayCatalogProvider;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\Column;
use Flow\PostgreSql\Schema\Constraint\PrimaryKey;
use Flow\PostgreSql\Schema\Constraint\UniqueConstraint;
use Flow\PostgreSql\Schema\Extension;
use Flow\PostgreSql\Schema\Func;
use Flow\PostgreSql\Schema\FunctionVolatility;
use Flow\PostgreSql\Schema\Index;
use Flow\PostgreSql\Schema\MaterializedView;
use Flow\PostgreSql\Schema\Procedure;
use Flow\PostgreSql\Schema\Schema;
use Flow\PostgreSql\Schema\Sequence;
use Flow\PostgreSql\Schema\Table;
use Flow\PostgreSql\Schema\View;
use PHPUnit\Framework\TestCase;

final class ArrayCatalogProviderTest extends TestCase
{
    public function test_creates_catalog_from_array_data(): void
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

        static::assertSame(['public'], $restored->names());
        static::assertTrue($restored->get('public')->hasTable('users'));
        static::assertNotNull($restored->get('public')->table('users')->primaryKey);
        static::assertSame('users_pkey', $restored->get('public')->table('users')->primaryKey->name);
        static::assertCount(2, $restored->get('public')->table('users')->columns);
    }

    public function test_creates_catalog_with_all_schema_objects(): void
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
                functions: [new Func(
                    'get_user',
                    'text',
                    ['integer'],
                    'sql',
                    'SELECT 1',
                    volatility: FunctionVolatility::STABLE,
                )],
                procedures: [new Procedure('cleanup', [], 'sql', 'DELETE FROM logs')],
                extensions: [new Extension('uuid-ossp', '1.1')],
            ),
        ]);

        $provider = new ArrayCatalogProvider($catalog->normalize());

        $restored = $provider->get();

        $schema = $restored->get('public');
        static::assertCount(1, $schema->tables);
        static::assertCount(1, $schema->sequences);
        static::assertCount(1, $schema->views);
        static::assertCount(1, $schema->materializedViews);
        static::assertCount(1, $schema->functions);
        static::assertCount(1, $schema->procedures);
        static::assertCount(1, $schema->extensions);
    }

    public function test_creates_empty_catalog(): void
    {
        $provider = new ArrayCatalogProvider(['schemas' => []]);

        static::assertSame([], $provider->get()->all());
    }
}
