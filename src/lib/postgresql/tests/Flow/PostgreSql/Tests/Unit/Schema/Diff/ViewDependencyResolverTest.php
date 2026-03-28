<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use function Flow\PostgreSql\DSL\ast_view_dependency_resolver;

use function Flow\PostgreSql\DSL\{schema, schema_column_integer, schema_column_text, schema_materialized_view, schema_table, schema_view};

use Flow\PostgreSql\Schema\{Catalog, MaterializedView, View};
use PHPUnit\Framework\TestCase;

final class ViewDependencyResolverTest extends TestCase
{
    public function test_does_not_resolve_view_depending_on_unmodified_table() : void
    {
        $catalog = new Catalog([
            schema(
                'public',
                tables: [
                    schema_table('users', [schema_column_integer('id', false)]),
                    schema_table('orders', [schema_column_integer('id', false)]),
                ],
                views: [schema_view('active_users', 'SELECT * FROM users')],
            ),
        ]);

        $resolver = ast_view_dependency_resolver();
        $result = $resolver->resolve($catalog, ['public.orders']);

        self::assertTrue($result->isEmpty());
    }

    public function test_resolves_cascading_view_dependencies() : void
    {
        $catalog = new Catalog([
            schema(
                'data',
                tables: [schema_table('events', [schema_column_integer('id', false), schema_column_text('type')])],
            ),
            schema(
                'report',
                views: [schema_view('event_summary', 'SELECT * FROM data.events')],
            ),
            schema(
                'public',
                views: [schema_view('dashboard', 'SELECT * FROM report.event_summary')],
            ),
        ]);

        $resolver = ast_view_dependency_resolver();
        $result = $resolver->resolve($catalog, ['data.events']);

        self::assertCount(2, $result->toDrop);
        self::assertSame('public.dashboard', $result->toDrop[0]->qualifiedName());
        self::assertSame('report.event_summary', $result->toDrop[1]->qualifiedName());

        self::assertCount(2, $result->toCreate);
        self::assertSame('report.event_summary', $result->toCreate[0]->qualifiedName());
        self::assertSame('public.dashboard', $result->toCreate[1]->qualifiedName());
    }

    public function test_resolves_cross_schema_dependency() : void
    {
        $catalog = new Catalog([
            schema(
                'enriched',
                tables: [schema_table('full_data', [schema_column_integer('id', false), schema_column_integer('amount')])],
            ),
            schema(
                'public',
                views: [schema_view('enriched_full_data', 'SELECT * FROM enriched.full_data')],
            ),
        ]);

        $resolver = ast_view_dependency_resolver();
        $result = $resolver->resolve($catalog, ['enriched.full_data']);

        self::assertCount(1, $result->toDrop);
        self::assertSame('public.enriched_full_data', $result->toDrop[0]->qualifiedName());
        self::assertInstanceOf(View::class, $result->toDrop[0]->view);

        self::assertCount(1, $result->toCreate);
        self::assertSame('public.enriched_full_data', $result->toCreate[0]->qualifiedName());
    }

    public function test_resolves_materialized_view_depending_on_modified_table() : void
    {
        $catalog = new Catalog([
            schema(
                'public',
                tables: [schema_table('orders', [schema_column_integer('id', false), schema_column_integer('total')])],
                materializedViews: [schema_materialized_view('mv_order_stats', 'SELECT * FROM orders')],
            ),
        ]);

        $resolver = ast_view_dependency_resolver();
        $result = $resolver->resolve($catalog, ['public.orders']);

        self::assertCount(1, $result->toDrop);
        self::assertSame('public.mv_order_stats', $result->toDrop[0]->qualifiedName());
        self::assertInstanceOf(MaterializedView::class, $result->toDrop[0]->view);

        self::assertCount(1, $result->toCreate);
        self::assertSame('public.mv_order_stats', $result->toCreate[0]->qualifiedName());
    }

    public function test_resolves_view_depending_on_modified_table() : void
    {
        $catalog = new Catalog([
            schema(
                'public',
                tables: [schema_table('users', [schema_column_integer('id', false), schema_column_text('name')])],
                views: [schema_view('active_users', 'SELECT * FROM users')],
            ),
        ]);

        $resolver = ast_view_dependency_resolver();
        $result = $resolver->resolve($catalog, ['public.users']);

        self::assertCount(1, $result->toDrop);
        self::assertSame('public.active_users', $result->toDrop[0]->qualifiedName());
        self::assertInstanceOf(View::class, $result->toDrop[0]->view);

        self::assertCount(1, $result->toCreate);
        self::assertSame('public.active_users', $result->toCreate[0]->qualifiedName());
    }

    public function test_returns_empty_when_no_modified_tables() : void
    {
        $catalog = new Catalog([
            schema(
                'public',
                tables: [schema_table('users', [schema_column_integer('id', false)])],
                views: [schema_view('active_users', 'SELECT * FROM users')],
            ),
        ]);

        $resolver = ast_view_dependency_resolver();
        $result = $resolver->resolve($catalog, []);

        self::assertTrue($result->isEmpty());
    }
}
