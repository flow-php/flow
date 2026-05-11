<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\Constraint\CheckConstraint;
use Flow\PostgreSql\Schema\Diff\ConstraintComparator;
use Flow\PostgreSql\Schema\Diff\GreedySimilarityRenameStrategy;
use Flow\PostgreSql\Schema\Diff\IndexComparator;
use Flow\PostgreSql\Schema\Diff\SchemaComparator;
use Flow\PostgreSql\Schema\Diff\SimilarTextStrategy;
use Flow\PostgreSql\Schema\Diff\TableComparator;
use Flow\PostgreSql\Schema\Diff\TableStructureComparator;
use Flow\PostgreSql\Schema\FunctionVolatility;
use Flow\PostgreSql\Schema\PartitionStrategy;
use Flow\PostgreSql\Schema\Schema;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_column_integer;
use function Flow\PostgreSql\DSL\schema_column_text;
use function Flow\PostgreSql\DSL\schema_domain;
use function Flow\PostgreSql\DSL\schema_extension;
use function Flow\PostgreSql\DSL\schema_function;
use function Flow\PostgreSql\DSL\schema_index;
use function Flow\PostgreSql\DSL\schema_materialized_view;
use function Flow\PostgreSql\DSL\schema_primary_key;
use function Flow\PostgreSql\DSL\schema_procedure;
use function Flow\PostgreSql\DSL\schema_sequence;
use function Flow\PostgreSql\DSL\schema_table;
use function Flow\PostgreSql\DSL\schema_view;

final class SchemaComparatorTest extends TestCase
{
    public function test_domain_added(): void
    {
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
        $source = new Schema('public');
        $target = new Schema('public', domains: [schema_domain('email', ColumnType::text())]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->addedDomains);
    }

    public function test_domain_modified_base_type(): void
    {
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
        $source = new Schema('public', domains: [schema_domain('d1', ColumnType::text())]);
        $target = new Schema('public', domains: [schema_domain('d1', ColumnType::varchar(255))]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedDomains);
    }

    public function test_domain_modified_check_constraint(): void
    {
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
        $source = new Schema('public', domains: [schema_domain('d1', ColumnType::text(), checkConstraints: [
            new CheckConstraint('VALUE ~ \'^[a-z]+$\'', 'chk_lower'),
        ])]);
        $target = new Schema('public', domains: [schema_domain('d1', ColumnType::text(), checkConstraints: [
            new CheckConstraint('VALUE ~ \'^[A-Z]+$\'', 'chk_lower'),
        ])]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedDomains);
    }

    public function test_domain_modified_default(): void
    {
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
        $source = new Schema('public', domains: [schema_domain('d1', ColumnType::text(), default: null)]);
        $target = new Schema('public', domains: [schema_domain('d1', ColumnType::text(), default: 'hello')]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedDomains);
    }

    public function test_domain_modified_nullable(): void
    {
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
        $source = new Schema('public', domains: [schema_domain('d1', ColumnType::text(), nullable: true)]);
        $target = new Schema('public', domains: [schema_domain('d1', ColumnType::text(), nullable: false)]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedDomains);
    }

    public function test_domain_removed(): void
    {
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
        $source = new Schema('public', domains: [schema_domain('email', ColumnType::text())]);
        $target = new Schema('public');

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->removedDomains);
    }

    public function test_extension_added(): void
    {
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
        $source = new Schema('public');
        $target = new Schema('public', extensions: [schema_extension('uuid-ossp')]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->addedExtensions);
    }

    public function test_extension_modified_version(): void
    {
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
        $source = new Schema('public', extensions: [schema_extension('uuid-ossp', version: '1.0')]);
        $target = new Schema('public', extensions: [schema_extension('uuid-ossp', version: '2.0')]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedExtensions);
    }

    public function test_extension_removed(): void
    {
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
        $source = new Schema('public', extensions: [schema_extension('uuid-ossp')]);
        $target = new Schema('public');

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->removedExtensions);
    }

    public function test_function_added(): void
    {
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
        $source = new Schema('public');
        $target = new Schema('public', functions: [schema_function(
            'add',
            'integer',
            ['integer', 'integer'],
            definition: 'SELECT $1 + $2',
        )]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->addedFunctions);
    }

    public function test_function_modified_argument_types(): void
    {
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
        $source = new Schema('public', functions: [schema_function('fn', 'integer', ['integer'])]);
        $target = new Schema('public', functions: [schema_function('fn', 'integer', ['text'])]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedFunctions);
    }

    public function test_function_modified_definition(): void
    {
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
        $source = new Schema('public', functions: [schema_function('fn', 'integer', definition: 'SELECT 1')]);
        $target = new Schema('public', functions: [schema_function('fn', 'integer', definition: 'SELECT 2')]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedFunctions);
    }

    public function test_function_modified_is_strict(): void
    {
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
        $source = new Schema('public', functions: [schema_function('fn', 'integer', isStrict: false)]);
        $target = new Schema('public', functions: [schema_function('fn', 'integer', isStrict: true)]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedFunctions);
    }

    public function test_function_modified_language(): void
    {
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
        $source = new Schema('public', functions: [schema_function('fn', 'integer', language: 'sql')]);
        $target = new Schema('public', functions: [schema_function('fn', 'integer', language: 'plpgsql')]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedFunctions);
    }

    public function test_function_modified_return_type(): void
    {
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
        $source = new Schema('public', functions: [schema_function('fn', 'integer')]);
        $target = new Schema('public', functions: [schema_function('fn', 'bigint')]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedFunctions);
    }

    public function test_function_modified_volatility(): void
    {
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
        $source = new Schema('public', functions: [schema_function(
            'fn',
            'integer',
            volatility: FunctionVolatility::VOLATILE,
        )]);
        $target = new Schema('public', functions: [schema_function(
            'fn',
            'integer',
            volatility: FunctionVolatility::STABLE,
        )]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedFunctions);
    }

    public function test_function_removed(): void
    {
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
        $source = new Schema('public', functions: [schema_function(
            'add',
            'integer',
            ['integer', 'integer'],
            definition: 'SELECT $1 + $2',
        )]);
        $target = new Schema('public');

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->removedFunctions);
    }

    public function test_identical_schemas_produce_empty_diff(): void
    {
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
        $schema = new Schema('public', tables: [
            schema_table('users', [schema_column_integer('id', false)]),
        ]);

        $diff = $comparator->compare($schema, $schema);

        static::assertTrue($diff->isEmpty());
    }

    public function test_materialized_view_added(): void
    {
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
        $source = new Schema('public');
        $target = new Schema('public', materializedViews: [schema_materialized_view(
            'mv_users',
            'SELECT * FROM users',
        )]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->addedMaterializedViews);
    }

    public function test_materialized_view_modified_definition(): void
    {
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
        $source = new Schema('public', materializedViews: [schema_materialized_view(
            'mv_users',
            'SELECT * FROM users',
        )]);
        $target = new Schema('public', materializedViews: [schema_materialized_view(
            'mv_users',
            'SELECT id FROM users',
        )]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedMaterializedViews);
    }

    public function test_materialized_view_modified_index(): void
    {
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
        $source = new Schema('public', materializedViews: [schema_materialized_view(
            'mv_users',
            'SELECT * FROM users',
        )]);
        $target = new Schema('public', materializedViews: [schema_materialized_view(
            'mv_users',
            'SELECT * FROM users',
            indexes: [
                schema_index('idx_mv_id', ['id']),
            ],
        )]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedMaterializedViews);
    }

    public function test_materialized_view_removed(): void
    {
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
        $source = new Schema('public', materializedViews: [schema_materialized_view(
            'mv_users',
            'SELECT * FROM users',
        )]);
        $target = new Schema('public');

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->removedMaterializedViews);
    }

    public function test_procedure_added(): void
    {
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
        $source = new Schema('public');
        $target = new Schema('public', procedures: [schema_procedure('do_stuff')]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->addedProcedures);
    }

    public function test_procedure_modified_argument_types(): void
    {
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
        $source = new Schema('public', procedures: [schema_procedure('do_stuff', argumentTypes: ['integer'])]);
        $target = new Schema('public', procedures: [schema_procedure('do_stuff', argumentTypes: ['text'])]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedProcedures);
    }

    public function test_procedure_modified_definition(): void
    {
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
        $source = new Schema('public', procedures: [schema_procedure('do_stuff', definition: 'BEGIN END')]);
        $target = new Schema('public', procedures: [schema_procedure(
            'do_stuff',
            definition: 'BEGIN RAISE NOTICE; END',
        )]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedProcedures);
    }

    public function test_procedure_modified_language(): void
    {
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
        $source = new Schema('public', procedures: [schema_procedure('do_stuff', language: 'sql')]);
        $target = new Schema('public', procedures: [schema_procedure('do_stuff', language: 'plpgsql')]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedProcedures);
    }

    public function test_procedure_removed(): void
    {
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
        $source = new Schema('public', procedures: [schema_procedure('do_stuff')]);
        $target = new Schema('public');

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->removedProcedures);
    }

    public function test_sequence_added(): void
    {
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
        $source = new Schema('public');
        $target = new Schema('public', sequences: [schema_sequence('users_id_seq')]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->addedSequences);
        static::assertSame('users_id_seq', $diff->addedSequences[0]->name);
    }

    public function test_sequence_modified_cache_value(): void
    {
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
        $source = new Schema('public', sequences: [schema_sequence('s1', cacheValue: 1)]);
        $target = new Schema('public', sequences: [schema_sequence('s1', cacheValue: 20)]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedSequences);
    }

    public function test_sequence_modified_cycle(): void
    {
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
        $source = new Schema('public', sequences: [schema_sequence('s1', cycle: false)]);
        $target = new Schema('public', sequences: [schema_sequence('s1', cycle: true)]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedSequences);
    }

    public function test_sequence_modified_data_type(): void
    {
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
        $source = new Schema('public', sequences: [schema_sequence('s1', dataType: 'bigint')]);
        $target = new Schema('public', sequences: [schema_sequence('s1', dataType: 'integer')]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedSequences);
    }

    public function test_sequence_modified_increment_by(): void
    {
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
        $source = new Schema('public', sequences: [schema_sequence('s1', incrementBy: 1)]);
        $target = new Schema('public', sequences: [schema_sequence('s1', incrementBy: 5)]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedSequences);
    }

    public function test_sequence_modified_max_value(): void
    {
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
        $source = new Schema('public', sequences: [schema_sequence('s1', maxValue: null)]);
        $target = new Schema('public', sequences: [schema_sequence('s1', maxValue: 999)]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedSequences);
    }

    public function test_sequence_modified_min_value(): void
    {
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
        $source = new Schema('public', sequences: [schema_sequence('s1', minValue: 1)]);
        $target = new Schema('public', sequences: [schema_sequence('s1', minValue: 10)]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedSequences);
    }

    public function test_sequence_modified_owned_by_column(): void
    {
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
        $source = new Schema('public', sequences: [schema_sequence('s1', ownedByColumn: null)]);
        $target = new Schema('public', sequences: [schema_sequence('s1', ownedByColumn: 'id')]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedSequences);
    }

    public function test_sequence_modified_owned_by_table(): void
    {
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
        $source = new Schema('public', sequences: [schema_sequence('s1', ownedByTable: null)]);
        $target = new Schema('public', sequences: [schema_sequence('s1', ownedByTable: 'users')]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedSequences);
    }

    public function test_sequence_modified_start_value(): void
    {
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
        $source = new Schema('public', sequences: [schema_sequence('s1', startValue: 1)]);
        $target = new Schema('public', sequences: [schema_sequence('s1', startValue: 100)]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedSequences);
    }

    public function test_sequence_removed(): void
    {
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
        $source = new Schema('public', sequences: [schema_sequence('users_id_seq')]);
        $target = new Schema('public');

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->removedSequences);
    }

    public function test_table_added(): void
    {
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
        $source = new Schema('public');
        $target = new Schema('public', tables: [schema_table('users', [schema_column_integer('id', false)])]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->addedTables);
        static::assertSame('users', $diff->addedTables[0]->name);
    }

    public function test_table_inherits_changed(): void
    {
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
        $source = new Schema('public', tables: [schema_table('employees', [schema_column_integer('id', false)])]);
        $target = new Schema('public', tables: [schema_table(
            'employees',
            [schema_column_integer('id', false)],
            inherits: ['persons'],
        )]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedTables);
        static::assertSame(['persons'], $diff->modifiedTables[0]->addedInherits);
        static::assertSame([], $diff->modifiedTables[0]->removedInherits);
    }

    public function test_table_modified(): void
    {
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
        $source = new Schema('public', tables: [schema_table('users', [schema_column_integer('id', false)])]);
        $target = new Schema('public', tables: [schema_table('users', [
            schema_column_integer('id', false),
            schema_column_text('email'),
        ])]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedTables);
    }

    public function test_table_partition_changed(): void
    {
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
        $source = new Schema('public', tables: [schema_table('events', [schema_column_integer('id', false)])]);
        $target = new Schema('public', tables: [schema_table(
            'events',
            [schema_column_integer('id', false)],
            partitionStrategy: PartitionStrategy::RANGE,
            partitionColumns: ['id'],
        )]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedTables);
        static::assertTrue($diff->modifiedTables[0]->partitionChanged);
    }

    public function test_table_removed(): void
    {
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
        $source = new Schema('public', tables: [schema_table('users', [schema_column_integer('id', false)])]);
        $target = new Schema('public');

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->removedTables);
        static::assertSame('users', $diff->removedTables[0]->name);
    }

    public function test_table_rename_populates_schema_diff(): void
    {
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
        $source = new Schema('public', tables: [
            schema_table(
                'old_users',
                [schema_column_integer('id', false), schema_column_text('name')],
                schema_primary_key(['id']),
            ),
        ]);
        $target = new Schema('public', tables: [
            schema_table(
                'new_users',
                [schema_column_integer('id', false), schema_column_text('name')],
                schema_primary_key(['id']),
            ),
        ]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(0, $diff->addedTables);
        static::assertCount(0, $diff->removedTables);
        static::assertCount(1, $diff->renamedTables);
        static::assertArrayHasKey('public.old_users', $diff->renamedTables);
        static::assertSame('new_users', $diff->renamedTables['public.old_users']->name);
    }

    public function test_table_tablespace_changed(): void
    {
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
        $source = new Schema('public', tables: [schema_table('users', [schema_column_integer('id', false)])]);
        $target = new Schema('public', tables: [schema_table(
            'users',
            [schema_column_integer('id', false)],
            tablespace: 'fast_storage',
        )]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedTables);
        static::assertTrue($diff->modifiedTables[0]->tablespaceChanged);
    }

    public function test_table_unlogged_changed(): void
    {
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
        $source = new Schema('public', tables: [schema_table('users', [schema_column_integer('id', false)])]);
        $target = new Schema('public', tables: [schema_table(
            'users',
            [schema_column_integer('id', false)],
            unlogged: true,
        )]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedTables);
        static::assertTrue($diff->modifiedTables[0]->unloggedChanged);
    }

    public function test_view_added(): void
    {
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
        $source = new Schema('public');
        $target = new Schema('public', views: [schema_view('v_users', 'SELECT * FROM users')]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->addedViews);
        static::assertSame('v_users', $diff->addedViews[0]->name);
    }

    public function test_view_modified_definition(): void
    {
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
        $source = new Schema('public', views: [schema_view('v_users', 'SELECT * FROM users')]);
        $target = new Schema('public', views: [schema_view('v_users', 'SELECT id FROM users')]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedViews);
    }

    public function test_view_modified_is_updatable(): void
    {
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
        $source = new Schema('public', views: [schema_view('v_users', 'SELECT * FROM users', isUpdatable: false)]);
        $target = new Schema('public', views: [schema_view('v_users', 'SELECT * FROM users', isUpdatable: true)]);

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->modifiedViews);
    }

    public function test_view_removed(): void
    {
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
        $source = new Schema('public', views: [schema_view('v_users', 'SELECT * FROM users')]);
        $target = new Schema('public');

        $diff = $comparator->compare($source, $target);

        static::assertCount(1, $diff->removedViews);
    }
}
