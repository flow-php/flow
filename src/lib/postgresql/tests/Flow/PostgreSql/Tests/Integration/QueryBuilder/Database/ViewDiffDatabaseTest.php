<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use Flow\PostgreSql\Client\Infrastructure\PgSql\PgCatalogProvider;
use Flow\PostgreSql\Schema\Diff\ConstraintComparator;
use Flow\PostgreSql\Schema\Diff\GreedySimilarityRenameStrategy;
use Flow\PostgreSql\Schema\Diff\IndexComparator;
use Flow\PostgreSql\Schema\Diff\SchemaComparator;
use Flow\PostgreSql\Schema\Diff\SimilarTextStrategy;
use Flow\PostgreSql\Schema\Diff\TableComparator;
use Flow\PostgreSql\Schema\Diff\TableStructureComparator;
use Flow\PostgreSql\Schema\MaterializedView;
use Flow\PostgreSql\Schema\Schema;
use Flow\PostgreSql\Schema\View;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function array_filter;
use function array_values;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_serial;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\schema_materialized_view;
use function Flow\PostgreSql\DSL\schema_view;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

final class ViewDiffDatabaseTest extends PostgreSqlTestCase
{
    private const MATVIEW_SIMPLE = 'flow_postgres_view_diff_matview';

    private const TABLE_SOURCE = 'flow_postgres_view_diff_source';

    private const VIEW_SIMPLE = 'flow_postgres_view_diff_view';

    protected function setUp(): void
    {
        parent::setUp();

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_SOURCE)
                    ->column(column('id', column_type_serial()))
                    ->column(column('name', column_type_varchar(100)))
                    ->toSql(),
            );
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropViewIfExists(self::VIEW_SIMPLE);
        $this->pgsqlContext()->dropMaterializedViewIfExists(self::MATVIEW_SIMPLE);
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_SOURCE);

        parent::tearDown();
    }

    public function test_catalog_materialized_view_matches_single_line_definition(): void
    {
        $select = select(col('id'), col('name'))->from(table(self::TABLE_SOURCE));
        $this
            ->pgsqlContext()
            ->client()
            ->execute(create()->materializedView(self::MATVIEW_SIMPLE)->as($select)->toSql());

        $catalogSchema = (new PgCatalogProvider($this->pgsqlContext()->client(), ['public']))->get()->get('public');
        $catalogMatViews = array_values(array_filter(
            $catalogSchema->materializedViews,
            static fn(MaterializedView $mv): bool => $mv->name === self::MATVIEW_SIMPLE,
        ));
        static::assertCount(1, $catalogMatViews);

        $constraintComparator = new ConstraintComparator();
        $comparator = new SchemaComparator(
            new TableComparator(
                new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())),
                $constraintComparator,
                new GreedySimilarityRenameStrategy(new SimilarTextStrategy()),
            ),
            new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())),
            $constraintComparator,
            new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())),
        );

        $diff = $comparator->compare(
            new Schema('public', materializedViews: [$catalogMatViews[0]]),
            new Schema('public', materializedViews: [
                schema_materialized_view(self::MATVIEW_SIMPLE, $select->toSql()),
            ]),
        );

        static::assertSame([], $diff->modifiedMaterializedViews);
    }

    public function test_catalog_view_matches_single_line_definition(): void
    {
        $select = select(col('id'), col('name'))->from(table(self::TABLE_SOURCE));
        $this->pgsqlContext()->client()->execute(create()->view(self::VIEW_SIMPLE)->as($select)->toSql());

        $catalogSchema = (new PgCatalogProvider($this->pgsqlContext()->client(), ['public']))->get()->get('public');
        $catalogViews = array_values(array_filter(
            $catalogSchema->views,
            static fn(View $v): bool => $v->name === self::VIEW_SIMPLE,
        ));
        static::assertCount(1, $catalogViews);

        $constraintComparator = new ConstraintComparator();
        $comparator = new SchemaComparator(
            new TableComparator(
                new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())),
                $constraintComparator,
                new GreedySimilarityRenameStrategy(new SimilarTextStrategy()),
            ),
            new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())),
            $constraintComparator,
            new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())),
        );

        $diff = $comparator->compare(
            new Schema('public', views: [$catalogViews[0]]),
            new Schema('public', views: [
                schema_view(self::VIEW_SIMPLE, $select->toSql(), isUpdatable: $catalogViews[0]->isUpdatable),
            ]),
        );

        static::assertSame([], $diff->modifiedViews);
    }
}
