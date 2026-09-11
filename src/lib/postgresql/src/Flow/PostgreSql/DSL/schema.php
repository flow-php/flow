<?php

declare(strict_types=1);

namespace Flow\PostgreSql\DSL;

use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type as DSLType;
use Flow\PostgreSql\Client;
use Flow\PostgreSql\Client\Infrastructure\PgSql\PgCatalogProvider;
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Parser\ColumnTypeParser;
use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Delete\DeleteBuilder;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Factory\AlterFactory;
use Flow\PostgreSql\QueryBuilder\Factory\CreateFactory;
use Flow\PostgreSql\QueryBuilder\Factory\DropFactory;
use Flow\PostgreSql\QueryBuilder\Insert\InsertBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnDefinition;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\CheckConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\ForeignKeyConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\PrimaryKeyConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\UniqueConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\Function\CallBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Function\CallFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Function\DoBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Function\DoFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Function\FunctionArgument;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\GrantBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\GrantOnStep;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\GrantRoleBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\GrantRoleToStep;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\RevokeBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\RevokeOnStep;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\RevokeRoleBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\RevokeRoleFromStep;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\TablePrivilege;
use Flow\PostgreSql\QueryBuilder\Schema\Index\IndexColumn;
use Flow\PostgreSql\QueryBuilder\Schema\Index\IndexMethod;
use Flow\PostgreSql\QueryBuilder\Schema\Index\Reindex\ReindexBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Index\Reindex\ReindexFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Ownership\DropOwnedBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Ownership\DropOwnedFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Ownership\ReassignOwnedBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Ownership\ReassignOwnedToStep;
use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;
use Flow\PostgreSql\QueryBuilder\Schema\Session\ResetRoleBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Session\ResetRoleFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Session\SetRoleBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Session\SetRoleFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Truncate\TruncateBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Truncate\TruncateFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Type\TypeAttribute;
use Flow\PostgreSql\QueryBuilder\Schema\View\RefreshMaterializedView\RefreshMaterializedViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\RefreshMaterializedView\RefreshMatViewOptionsStep;
use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;
use Flow\PostgreSql\QueryBuilder\Update\UpdateBuilder;
use Flow\PostgreSql\QueryBuilder\Utility\AnalyzeBuilder;
use Flow\PostgreSql\QueryBuilder\Utility\AnalyzeFinalStep;
use Flow\PostgreSql\QueryBuilder\Utility\ClusterBuilder;
use Flow\PostgreSql\QueryBuilder\Utility\ClusterFinalStep;
use Flow\PostgreSql\QueryBuilder\Utility\CommentBuilder;
use Flow\PostgreSql\QueryBuilder\Utility\CommentFinalStep;
use Flow\PostgreSql\QueryBuilder\Utility\CommentTarget;
use Flow\PostgreSql\QueryBuilder\Utility\DiscardBuilder;
use Flow\PostgreSql\QueryBuilder\Utility\DiscardFinalStep;
use Flow\PostgreSql\QueryBuilder\Utility\DiscardType;
use Flow\PostgreSql\QueryBuilder\Utility\ExplainBuilder;
use Flow\PostgreSql\QueryBuilder\Utility\ExplainFinalStep;
use Flow\PostgreSql\QueryBuilder\Utility\LockBuilder;
use Flow\PostgreSql\QueryBuilder\Utility\LockFinalStep;
use Flow\PostgreSql\QueryBuilder\Utility\VacuumBuilder;
use Flow\PostgreSql\QueryBuilder\Utility\VacuumFinalStep;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\CatalogProvider;
use Flow\PostgreSql\Schema\ChainCatalogProvider;
use Flow\PostgreSql\Schema\Column as SchemaColumn;
use Flow\PostgreSql\Schema\Constraint\CheckConstraint as SchemaCheckConstraint;
use Flow\PostgreSql\Schema\Constraint\ExcludeConstraint as SchemaExcludeConstraint;
use Flow\PostgreSql\Schema\Constraint\ForeignKey as SchemaForeignKey;
use Flow\PostgreSql\Schema\Constraint\PrimaryKey as SchemaPrimaryKey;
use Flow\PostgreSql\Schema\Constraint\UniqueConstraint as SchemaUniqueConstraint;
use Flow\PostgreSql\Schema\Diff\AstViewDependencyResolver;
use Flow\PostgreSql\Schema\Diff\CatalogComparator;
use Flow\PostgreSql\Schema\Diff\NoopViewDependencyResolver;
use Flow\PostgreSql\Schema\Diff\RenameStrategy;
use Flow\PostgreSql\Schema\Diff\ViewDependencyResolver;
use Flow\PostgreSql\Schema\Domain as SchemaDomain;
use Flow\PostgreSql\Schema\Exclusion\AnyExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\EndsWithExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\ExactMatchExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\ExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\PatternExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\SchemaObjectType;
use Flow\PostgreSql\Schema\Exclusion\ScopedExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\StartsWithExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\WholeSchemaExclusionPolicy;
use Flow\PostgreSql\Schema\ExecutionOrderStrategy;
use Flow\PostgreSql\Schema\Extension as SchemaExtension;
use Flow\PostgreSql\Schema\ForeignKeyDependencyOrder;
use Flow\PostgreSql\Schema\Func as SchemaFunction;
use Flow\PostgreSql\Schema\FunctionVolatility as SchemaFunctionVolatility;
use Flow\PostgreSql\Schema\IdentityGeneration;
use Flow\PostgreSql\Schema\Index as SchemaIndex;
use Flow\PostgreSql\Schema\IndexMethod as SchemaIndexMethod;
use Flow\PostgreSql\Schema\ManualCatalogProvider;
use Flow\PostgreSql\Schema\MaterializedView as SchemaMaterializedView;
use Flow\PostgreSql\Schema\MaterializedViewDependencyOrder;
use Flow\PostgreSql\Schema\NoExecutionOrder;
use Flow\PostgreSql\Schema\PartitionStrategy;
use Flow\PostgreSql\Schema\Procedure as SchemaProcedure;
use Flow\PostgreSql\Schema\Schema as DatabaseSchema;
use Flow\PostgreSql\Schema\Schema;
use Flow\PostgreSql\Schema\Sequence as SchemaSequence;
use Flow\PostgreSql\Schema\Table as SchemaTable;
use Flow\PostgreSql\Schema\TableOptions as SchemaTableOptions;
use Flow\PostgreSql\Schema\Trigger as SchemaTrigger;
use Flow\PostgreSql\Schema\TriggerEvent;
use Flow\PostgreSql\Schema\TriggerTiming;
use Flow\PostgreSql\Schema\View as SchemaView;
use Flow\PostgreSql\Schema\ViewDependencyOrder;

/**
 * Create a column definition for CREATE TABLE.
 *
 * @param string $name Column name
 * @param ColumnType $type Column data type
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column(string $name, ColumnType $type): ColumnDefinition
{
    return ColumnDefinition::create($name, $type);
}

/**
 * @param list<Schema> $schemas
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function catalog(array $schemas): Catalog
{
    return new Catalog($schemas);
}

/**
 * Create a PRIMARY KEY constraint.
 *
 * @param string ...$columns Columns that form the primary key
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function primary_key(string ...$columns): PrimaryKeyConstraint
{
    return PrimaryKeyConstraint::create(...$columns);
}

/**
 * Create a UNIQUE constraint.
 *
 * @param string ...$columns Columns that must be unique together
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function unique_constraint(string ...$columns): UniqueConstraint
{
    return UniqueConstraint::create(...$columns);
}

/**
 * Create a FOREIGN KEY constraint.
 *
 * @param list<string> $columns Local columns
 * @param string $referenceTable Referenced table
 * @param list<string> $referenceColumns Referenced columns (defaults to same as $columns if empty)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function foreign_key(array $columns, string $referenceTable, array $referenceColumns = []): ForeignKeyConstraint
{
    return ForeignKeyConstraint::create($columns, $referenceTable, $referenceColumns);
}

/**
 * Create a CHECK constraint.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function check_constraint(Condition $condition): CheckConstraint
{
    return CheckConstraint::create($condition);
}

/**
 * Create a factory for building CREATE statements.
 *
 * Provides a unified entry point for all CREATE operations:
 * - create()->table() - CREATE TABLE
 * - create()->tableAs() - CREATE TABLE AS
 * - create()->index() - CREATE INDEX
 * - create()->view() - CREATE VIEW
 * - create()->materializedView() - CREATE MATERIALIZED VIEW
 * - create()->sequence() - CREATE SEQUENCE
 * - create()->schema() - CREATE SCHEMA
 * - create()->role() - CREATE ROLE
 * - create()->function() - CREATE FUNCTION
 * - create()->procedure() - CREATE PROCEDURE
 * - create()->trigger() - CREATE TRIGGER
 * - create()->rule() - CREATE RULE
 * - create()->extension() - CREATE EXTENSION
 * - create()->compositeType() - CREATE TYPE (composite)
 * - create()->enumType() - CREATE TYPE (enum)
 * - create()->rangeType() - CREATE TYPE (range)
 * - create()->domain() - CREATE DOMAIN
 *
 * Example: create()->table('users')->columns(col_def('id', column_type_serial()))
 * Example: create()->index('idx_email')->on('users')->columns('email')
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function create(): CreateFactory
{
    return new CreateFactory();
}

/**
 * Create a factory for building DROP statements.
 *
 * Provides a unified entry point for all DROP operations:
 * - drop()->table() - DROP TABLE
 * - drop()->index() - DROP INDEX
 * - drop()->view() - DROP VIEW
 * - drop()->materializedView() - DROP MATERIALIZED VIEW
 * - drop()->sequence() - DROP SEQUENCE
 * - drop()->schema() - DROP SCHEMA
 * - drop()->role() - DROP ROLE
 * - drop()->function() - DROP FUNCTION
 * - drop()->procedure() - DROP PROCEDURE
 * - drop()->trigger() - DROP TRIGGER
 * - drop()->rule() - DROP RULE
 * - drop()->extension() - DROP EXTENSION
 * - drop()->type() - DROP TYPE
 * - drop()->domain() - DROP DOMAIN
 * - drop()->owned() - DROP OWNED
 *
 * Example: drop()->table('users', 'orders')->ifExists()->cascade()
 * Example: drop()->index('idx_email')->ifExists()
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function drop(): DropFactory
{
    return new DropFactory();
}

/**
 * Create a factory for building ALTER statements.
 *
 * Provides a unified entry point for all ALTER operations:
 * - alter()->table() - ALTER TABLE
 * - alter()->index() - ALTER INDEX
 * - alter()->view() - ALTER VIEW
 * - alter()->materializedView() - ALTER MATERIALIZED VIEW
 * - alter()->sequence() - ALTER SEQUENCE
 * - alter()->schema() - ALTER SCHEMA
 * - alter()->role() - ALTER ROLE
 * - alter()->function() - ALTER FUNCTION
 * - alter()->procedure() - ALTER PROCEDURE
 * - alter()->trigger() - ALTER TRIGGER
 * - alter()->extension() - ALTER EXTENSION
 * - alter()->enumType() - ALTER TYPE (enum)
 * - alter()->domain() - ALTER DOMAIN
 *
 * Rename operations are also under alter():
 * - alter()->index('old')->renameTo('new')
 * - alter()->view('old')->renameTo('new')
 * - alter()->schema('old')->renameTo('new')
 * - alter()->role('old')->renameTo('new')
 * - alter()->trigger('old')->on('table')->renameTo('new')
 *
 * Example: alter()->table('users')->addColumn(col_def('email', column_type_text()))
 * Example: alter()->sequence('user_id_seq')->restart(1000)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function alter(): AlterFactory
{
    return new AlterFactory();
}

/**
 * Create a TRUNCATE TABLE builder.
 *
 * @param string ...$tables Table names to truncate
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function truncate_table(string ...$tables): TruncateFinalStep
{
    return TruncateBuilder::create(...$tables);
}

/**
 * Create a REFRESH MATERIALIZED VIEW builder.
 *
 * Example: refresh_materialized_view('user_stats')
 * Produces: REFRESH MATERIALIZED VIEW user_stats
 *
 * Example: refresh_materialized_view('user_stats')->concurrently()->withData()
 * Produces: REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats WITH DATA
 *
 * @param string $name View name (may include schema as "schema.view")
 * @param null|string $schema Schema name (optional, overrides parsed schema)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function refresh_materialized_view(string $name, ?string $schema = null): RefreshMatViewOptionsStep
{
    return RefreshMaterializedViewBuilder::create($name, $schema);
}

/**
 * Get a CASCADE referential action.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function ref_action_cascade(): ReferentialAction
{
    return ReferentialAction::CASCADE;
}

/**
 * Get a RESTRICT referential action.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function ref_action_restrict(): ReferentialAction
{
    return ReferentialAction::RESTRICT;
}

/**
 * Get a SET NULL referential action.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function ref_action_set_null(): ReferentialAction
{
    return ReferentialAction::SET_NULL;
}

/**
 * Get a SET DEFAULT referential action.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function ref_action_set_default(): ReferentialAction
{
    return ReferentialAction::SET_DEFAULT;
}

/**
 * Get a NO ACTION referential action.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function ref_action_no_action(): ReferentialAction
{
    return ReferentialAction::NO_ACTION;
}

/**
 * Start building a REINDEX INDEX statement.
 *
 * Use chainable methods: ->concurrently(), ->verbose(), ->tablespace()
 *
 * Example: reindex_index('idx_users_email')->concurrently()
 *
 * @param string $name The index name (may include schema: schema.index)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function reindex_index(string $name): ReindexFinalStep
{
    return ReindexBuilder::index($name);
}

/**
 * Start building a REINDEX TABLE statement.
 *
 * Use chainable methods: ->concurrently(), ->verbose(), ->tablespace()
 *
 * Example: reindex_table('users')->concurrently()
 *
 * @param string $name The table name (may include schema: schema.table)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function reindex_table(string $name): ReindexFinalStep
{
    return ReindexBuilder::table($name);
}

/**
 * Start building a REINDEX SCHEMA statement.
 *
 * Use chainable methods: ->concurrently(), ->verbose(), ->tablespace()
 *
 * Example: reindex_schema('public')->concurrently()
 *
 * @param string $name The schema name
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function reindex_schema(string $name): ReindexFinalStep
{
    return ReindexBuilder::schema($name);
}

/**
 * Start building a REINDEX DATABASE statement.
 *
 * Use chainable methods: ->concurrently(), ->verbose(), ->tablespace()
 *
 * Example: reindex_database('mydb')->concurrently()
 *
 * @param string $name The database name
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function reindex_database(string $name): ReindexFinalStep
{
    return ReindexBuilder::database($name);
}

/**
 * Create an index column specification.
 *
 * Use chainable methods: ->asc(), ->desc(), ->nullsFirst(), ->nullsLast(), ->opclass(), ->collate()
 *
 * Example: index_col('email')->desc()->nullsLast()
 *
 * @param string $name The column name
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function index_col(string $name): IndexColumn
{
    return IndexColumn::column($name);
}

/**
 * Create an index column specification from an expression.
 *
 * Use chainable methods: ->asc(), ->desc(), ->nullsFirst(), ->nullsLast(), ->opclass(), ->collate()
 *
 * Example: index_expr(fn_call('lower', col('email')))->desc()
 *
 * @param Expression $expression The expression to index
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function index_expr(Expression $expression): IndexColumn
{
    return IndexColumn::expression($expression);
}

/**
 * Get the BTREE index method.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function index_method_btree(): IndexMethod
{
    return IndexMethod::BTREE;
}

/**
 * Get the HASH index method.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function index_method_hash(): IndexMethod
{
    return IndexMethod::HASH;
}

/**
 * Get the GIST index method.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function index_method_gist(): IndexMethod
{
    return IndexMethod::GIST;
}

/**
 * Get the SPGIST index method.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function index_method_spgist(): IndexMethod
{
    return IndexMethod::SPGIST;
}

/**
 * Get the GIN index method.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function index_method_gin(): IndexMethod
{
    return IndexMethod::GIN;
}

/**
 * Get the BRIN index method.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function index_method_brin(): IndexMethod
{
    return IndexMethod::BRIN;
}

/**
 * Create a VACUUM builder.
 *
 * Example: vacuum()->table('users')
 * Produces: VACUUM users
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function vacuum(): VacuumFinalStep
{
    return VacuumBuilder::create();
}

/**
 * Create an ANALYZE builder.
 *
 * Example: analyze()->table('users')
 * Produces: ANALYZE users
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function analyze(): AnalyzeFinalStep
{
    return AnalyzeBuilder::create();
}

/**
 * Create an EXPLAIN builder for a query.
 *
 * Example: explain(select()->from('users'))
 * Produces: EXPLAIN SELECT * FROM users
 *
 * @param DeleteBuilder|InsertBuilder|SelectFinalStep|UpdateBuilder $query Query to explain
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function explain(SelectFinalStep|InsertBuilder|UpdateBuilder|DeleteBuilder $query): ExplainFinalStep
{
    return ExplainBuilder::create($query);
}

/**
 * Create a LOCK TABLE builder.
 *
 * Example: lock_table('users', 'orders')->accessExclusive()
 * Produces: LOCK TABLE users, orders IN ACCESS EXCLUSIVE MODE
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function lock_table(string ...$tables): LockFinalStep
{
    return LockBuilder::create(...$tables);
}

/**
 * Create a COMMENT ON builder.
 *
 * Example: comment(CommentTarget::TABLE, 'users')->is('User accounts table')
 * Produces: COMMENT ON TABLE users IS 'User accounts table'
 *
 * @param CommentTarget $target Target type (TABLE, COLUMN, INDEX, etc.)
 * @param string $name Target name (use 'table.column' for COLUMN targets)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function comment(CommentTarget $target, string $name): CommentFinalStep
{
    return CommentBuilder::create($target, $name);
}

/**
 * Create a CLUSTER builder.
 *
 * Example: cluster()->table('users')->using('idx_users_pkey')
 * Produces: CLUSTER users USING idx_users_pkey
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function cluster(): ClusterFinalStep
{
    return ClusterBuilder::create();
}

/**
 * Create a DISCARD builder.
 *
 * Example: discard(DiscardType::ALL)
 * Produces: DISCARD ALL
 *
 * @param DiscardType $type Type of resources to discard (ALL, PLANS, SEQUENCES, TEMP)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function discard(DiscardType $type): DiscardFinalStep
{
    return DiscardBuilder::create($type);
}

/**
 * Create a GRANT privileges builder.
 *
 * Example: grant(TablePrivilege::SELECT)->onTable('users')->to('app_user')
 * Produces: GRANT SELECT ON users TO app_user
 *
 * Example: grant(TablePrivilege::ALL)->onAllTablesInSchema('public')->to('admin')
 * Produces: GRANT ALL ON ALL TABLES IN SCHEMA public TO admin
 *
 * @param string|TablePrivilege ...$privileges The privileges to grant
 *
 * @return GrantOnStep Builder for grant options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function grant(TablePrivilege|string ...$privileges): GrantOnStep
{
    return GrantBuilder::create(...$privileges);
}

/**
 * Create a GRANT role builder.
 *
 * Example: grant_role('admin')->to('user1')
 * Produces: GRANT admin TO user1
 *
 * Example: grant_role('admin', 'developer')->to('user1')->withAdminOption()
 * Produces: GRANT admin, developer TO user1 WITH ADMIN OPTION
 *
 * @param string ...$roles The roles to grant
 *
 * @return GrantRoleToStep Builder for grant role options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function grant_role(string ...$roles): GrantRoleToStep
{
    return GrantRoleBuilder::create(...$roles);
}

/**
 * Create a REVOKE privileges builder.
 *
 * Example: revoke(TablePrivilege::SELECT)->onTable('users')->from('app_user')
 * Produces: REVOKE SELECT ON users FROM app_user
 *
 * Example: revoke(TablePrivilege::ALL)->onTable('users')->from('app_user')->cascade()
 * Produces: REVOKE ALL ON users FROM app_user CASCADE
 *
 * @param string|TablePrivilege ...$privileges The privileges to revoke
 *
 * @return RevokeOnStep Builder for revoke options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function revoke(TablePrivilege|string ...$privileges): RevokeOnStep
{
    return RevokeBuilder::create(...$privileges);
}

/**
 * Create a REVOKE role builder.
 *
 * Example: revoke_role('admin')->from('user1')
 * Produces: REVOKE admin FROM user1
 *
 * Example: revoke_role('admin')->from('user1')->cascade()
 * Produces: REVOKE admin FROM user1 CASCADE
 *
 * @param string ...$roles The roles to revoke
 *
 * @return RevokeRoleFromStep Builder for revoke role options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function revoke_role(string ...$roles): RevokeRoleFromStep
{
    return RevokeRoleBuilder::create(...$roles);
}

/**
 * Create a SET ROLE builder.
 *
 * Example: set_role('admin')
 * Produces: SET ROLE admin
 *
 * @param string $role The role to set
 *
 * @return SetRoleFinalStep Builder for set role
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function set_role(string $role): SetRoleFinalStep
{
    return SetRoleBuilder::create($role);
}

/**
 * Create a RESET ROLE builder.
 *
 * Example: reset_role()
 * Produces: RESET ROLE
 *
 * @return ResetRoleFinalStep Builder for reset role
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function reset_role(): ResetRoleFinalStep
{
    return ResetRoleBuilder::create();
}

/**
 * Create a REASSIGN OWNED builder.
 *
 * Example: reassign_owned('old_role')->to('new_role')
 * Produces: REASSIGN OWNED BY old_role TO new_role
 *
 * @param string ...$roles The roles whose owned objects should be reassigned
 *
 * @return ReassignOwnedToStep Builder for reassign owned options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function reassign_owned(string ...$roles): ReassignOwnedToStep
{
    return ReassignOwnedBuilder::create(...$roles);
}

/**
 * Create a DROP OWNED builder.
 *
 * Example: drop_owned('role1')
 * Produces: DROP OWNED BY role1
 *
 * Example: drop_owned('role1', 'role2')->cascade()
 * Produces: DROP OWNED BY role1, role2 CASCADE
 *
 * @param string ...$roles The roles whose owned objects should be dropped
 *
 * @return DropOwnedFinalStep Builder for drop owned options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function drop_owned(string ...$roles): DropOwnedFinalStep
{
    return DropOwnedBuilder::create(...$roles);
}

/**
 * Creates a new function argument for use in function/procedure definitions.
 *
 * Example: func_arg(column_type_integer())
 * Example: func_arg(column_type_text())->named('username')
 * Example: func_arg(column_type_integer())->named('count')->default('0')
 * Example: func_arg(column_type_text())->out()
 *
 * @param ColumnType $type The PostgreSQL data type for the argument
 *
 * @return FunctionArgument Builder for function argument options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function func_arg(ColumnType $type): FunctionArgument
{
    return FunctionArgument::of($type);
}

/**
 * Creates a CALL statement builder for invoking a procedure.
 *
 * Example: call('update_stats')->with(123)
 * Produces: CALL update_stats(123)
 *
 * Example: call('process_data')->with('test', 42, true)
 * Produces: CALL process_data('test', 42, true)
 *
 * @param string $procedure The name of the procedure to call
 *
 * @return CallFinalStep Builder for call statement options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function call(string $procedure): CallFinalStep
{
    return CallBuilder::create($procedure);
}

/**
 * Creates a DO statement builder for executing an anonymous code block.
 *
 * Example: do_block('BEGIN RAISE NOTICE $$Hello World$$; END;')
 * Produces: DO $$ BEGIN RAISE NOTICE $$Hello World$$; END; $$ LANGUAGE plpgsql
 *
 * Example: do_block('SELECT 1')->language('sql')
 * Produces: DO $$ SELECT 1 $$ LANGUAGE sql
 *
 * @param string $code The anonymous code block to execute
 *
 * @return DoFinalStep Builder for DO statement options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function do_block(string $code): DoFinalStep
{
    return DoBuilder::create($code);
}

/**
 * Creates a type attribute for composite types.
 *
 * Example: type_attr('name', column_type_text())
 * Produces: name text
 *
 * Example: type_attr('description', column_type_text())->collate('en_US')
 * Produces: description text COLLATE "en_US"
 *
 * @param string $name The attribute name
 * @param ColumnType $type The attribute type
 *
 * @return TypeAttribute Type attribute value object
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function type_attr(string $name, ColumnType $type): TypeAttribute
{
    return TypeAttribute::of($name, $type);
}

/**
 * Create an integer data type (PostgreSQL int4).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_integer(): ColumnType
{
    return ColumnType::integer();
}

/**
 * Create a smallint data type (PostgreSQL int2).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_smallint(): ColumnType
{
    return ColumnType::smallint();
}

/**
 * Create a bigint data type (PostgreSQL int8).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_bigint(): ColumnType
{
    return ColumnType::bigint();
}

/**
 * Create a boolean data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_boolean(): ColumnType
{
    return ColumnType::boolean();
}

/**
 * Create a text data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_text(): ColumnType
{
    return ColumnType::text();
}

/**
 * Create a varchar data type with length constraint.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_varchar(int $length): ColumnType
{
    return ColumnType::varchar($length);
}

/**
 * Create a char data type with length constraint.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_char(int $length): ColumnType
{
    return ColumnType::char($length);
}

/**
 * Create a numeric data type with optional precision and scale.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_numeric(?int $precision = null, ?int $scale = null): ColumnType
{
    return ColumnType::numeric($precision, $scale);
}

/**
 * Create a decimal data type with optional precision and scale.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_decimal(?int $precision = null, ?int $scale = null): ColumnType
{
    return ColumnType::decimal($precision, $scale);
}

/**
 * Create a real data type (PostgreSQL float4).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_real(): ColumnType
{
    return ColumnType::real();
}

/**
 * Create a double precision data type (PostgreSQL float8).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_double_precision(): ColumnType
{
    return ColumnType::doublePrecision();
}

/**
 * Create a date data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_date(): ColumnType
{
    return ColumnType::date();
}

/**
 * Create a time data type with optional precision.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_time(?int $precision = null): ColumnType
{
    return ColumnType::time($precision);
}

/**
 * Create a timestamp data type with optional precision.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_timestamp(?int $precision = null): ColumnType
{
    return ColumnType::timestamp($precision);
}

/**
 * Create a timestamp with time zone data type with optional precision.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_timestamptz(?int $precision = null): ColumnType
{
    return ColumnType::timestamptz($precision);
}

/**
 * Create an interval data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_interval(): ColumnType
{
    return ColumnType::interval();
}

/**
 * Create a UUID data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_uuid(): ColumnType
{
    return ColumnType::uuid();
}

/**
 * Create a JSON data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_json(): ColumnType
{
    return ColumnType::json();
}

/**
 * Create a JSONB data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_jsonb(): ColumnType
{
    return ColumnType::jsonb();
}

/**
 * Create a bytea data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_bytea(): ColumnType
{
    return ColumnType::bytea();
}

/**
 * Create an XML data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_xml(): ColumnType
{
    return ColumnType::xml();
}

/**
 * Create an inet data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_inet(): ColumnType
{
    return ColumnType::inet();
}

/**
 * Create a cidr data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_cidr(): ColumnType
{
    return ColumnType::cidr();
}

/**
 * Create a macaddr data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_macaddr(): ColumnType
{
    return ColumnType::macaddr();
}

/**
 * Create a serial data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_serial(): ColumnType
{
    return ColumnType::serial();
}

/**
 * Create a smallserial data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_smallserial(): ColumnType
{
    return ColumnType::smallserial();
}

/**
 * Create a bigserial data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_bigserial(): ColumnType
{
    return ColumnType::bigserial();
}

/**
 * Create an array data type from an element type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_array(ColumnType $elementType): ColumnType
{
    return ColumnType::array($elementType);
}

/**
 * Create a custom data type.
 *
 * @param string $typeName Type name
 * @param null|string $schema Optional schema name
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_custom(string $typeName, ?string $schema = null): ColumnType
{
    return ColumnType::custom($typeName, $schema);
}

/**
 * Parse a PostgreSQL type string into a ColumnType.
 *
 * Handles all PostgreSQL type syntax including precision, arrays, and schema-qualified types.
 *
 * @param string $typeName PostgreSQL type string (e.g., 'integer', 'character varying(255)', 'text[]')
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column_type_from_string(string $typeName): ColumnType
{
    return (new ColumnTypeParser())->parse($typeName);
}

// ValueType DSL functions - Scalar types

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_text(): ValueType
{
    return ValueType::TEXT;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_varchar(): ValueType
{
    return ValueType::VARCHAR;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_char(): ValueType
{
    return ValueType::CHAR;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_bpchar(): ValueType
{
    return ValueType::BPCHAR;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_int2(): ValueType
{
    return ValueType::INT2;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_smallint(): ValueType
{
    return ValueType::INT2;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_int4(): ValueType
{
    return ValueType::INT4;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_integer(): ValueType
{
    return ValueType::INT4;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_int8(): ValueType
{
    return ValueType::INT8;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_bigint(): ValueType
{
    return ValueType::INT8;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_float4(): ValueType
{
    return ValueType::FLOAT4;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_real(): ValueType
{
    return ValueType::FLOAT4;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_float8(): ValueType
{
    return ValueType::FLOAT8;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_double(): ValueType
{
    return ValueType::FLOAT8;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_numeric(): ValueType
{
    return ValueType::NUMERIC;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_money(): ValueType
{
    return ValueType::MONEY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_bool(): ValueType
{
    return ValueType::BOOL;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_boolean(): ValueType
{
    return ValueType::BOOL;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_bytea(): ValueType
{
    return ValueType::BYTEA;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_bit(): ValueType
{
    return ValueType::BIT;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_varbit(): ValueType
{
    return ValueType::VARBIT;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_date(): ValueType
{
    return ValueType::DATE;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_time(): ValueType
{
    return ValueType::TIME;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_timetz(): ValueType
{
    return ValueType::TIMETZ;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_timestamp(): ValueType
{
    return ValueType::TIMESTAMP;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_timestamptz(): ValueType
{
    return ValueType::TIMESTAMPTZ;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_interval(): ValueType
{
    return ValueType::INTERVAL;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_json(): ValueType
{
    return ValueType::JSON;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_jsonb(): ValueType
{
    return ValueType::JSONB;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_uuid(): ValueType
{
    return ValueType::UUID;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_inet(): ValueType
{
    return ValueType::INET;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_cidr(): ValueType
{
    return ValueType::CIDR;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_macaddr(): ValueType
{
    return ValueType::MACADDR;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_macaddr8(): ValueType
{
    return ValueType::MACADDR8;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_xml(): ValueType
{
    return ValueType::XML;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_oid(): ValueType
{
    return ValueType::OID;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_text_array(): ValueType
{
    return ValueType::TEXT_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_varchar_array(): ValueType
{
    return ValueType::VARCHAR_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_int2_array(): ValueType
{
    return ValueType::INT2_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_int4_array(): ValueType
{
    return ValueType::INT4_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_int8_array(): ValueType
{
    return ValueType::INT8_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_float4_array(): ValueType
{
    return ValueType::FLOAT4_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_float8_array(): ValueType
{
    return ValueType::FLOAT8_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_bool_array(): ValueType
{
    return ValueType::BOOL_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_uuid_array(): ValueType
{
    return ValueType::UUID_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_json_array(): ValueType
{
    return ValueType::JSON_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function value_type_jsonb_array(): ValueType
{
    return ValueType::JSONB_ARRAY;
}

/**
 * @param list<SchemaTable> $tables
 * @param list<SchemaSequence> $sequences
 * @param list<SchemaView> $views
 * @param list<SchemaMaterializedView> $materializedViews
 * @param list<SchemaFunction> $functions
 * @param list<SchemaProcedure> $procedures
 * @param list<SchemaDomain> $domains
 * @param list<SchemaExtension> $extensions
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema(
    string $name,
    array $tables = [],
    array $sequences = [],
    array $views = [],
    array $materializedViews = [],
    array $functions = [],
    array $procedures = [],
    array $domains = [],
    array $extensions = [],
): DatabaseSchema {
    return new DatabaseSchema(
        $name,
        $tables,
        $sequences,
        $views,
        $materializedViews,
        $functions,
        $procedures,
        $domains,
        $extensions,
    );
}

/**
 * @param non-empty-list<SchemaColumn> $columns
 * @param list<SchemaIndex> $indexes
 * @param list<SchemaForeignKey> $foreignKeys
 * @param list<SchemaUniqueConstraint> $uniqueConstraints
 * @param list<SchemaCheckConstraint> $checkConstraints
 * @param list<SchemaExcludeConstraint> $excludeConstraints
 * @param list<SchemaTrigger> $triggers
 * @param list<string> $partitionColumns
 * @param list<string> $inherits
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_table(
    string $name,
    array $columns,
    ?SchemaPrimaryKey $primaryKey = null,
    array $indexes = [],
    array $foreignKeys = [],
    array $uniqueConstraints = [],
    array $checkConstraints = [],
    array $excludeConstraints = [],
    array $triggers = [],
    string $schema = 'public',
    bool $unlogged = false,
    ?PartitionStrategy $partitionStrategy = null,
    array $partitionColumns = [],
    array $inherits = [],
    ?string $tablespace = null,
): SchemaTable {
    return new SchemaTable(
        $schema,
        $name,
        $columns,
        $primaryKey,
        $indexes,
        $foreignKeys,
        $uniqueConstraints,
        $checkConstraints,
        $excludeConstraints,
        $triggers,
        $unlogged,
        $partitionStrategy,
        $partitionColumns,
        $inherits,
        $tablespace,
    );
}

/**
 * @param list<SchemaForeignKey> $foreignKeys
 * @param list<SchemaCheckConstraint> $checkConstraints
 * @param list<SchemaExcludeConstraint> $excludeConstraints
 * @param list<SchemaTrigger> $triggers
 * @param list<string> $partitionColumns
 * @param list<string> $inherits
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_table_options(
    array $foreignKeys = [],
    array $checkConstraints = [],
    array $excludeConstraints = [],
    array $triggers = [],
    bool $unlogged = false,
    ?PartitionStrategy $partitionStrategy = null,
    array $partitionColumns = [],
    array $inherits = [],
    ?string $tablespace = null,
): SchemaTableOptions {
    return new SchemaTableOptions(
        $foreignKeys,
        $checkConstraints,
        $excludeConstraints,
        $triggers,
        $unlogged,
        $partitionStrategy,
        $partitionColumns,
        $inherits,
        $tablespace,
    );
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column(
    string $name,
    ColumnType $type,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
    bool $isIdentity = false,
    ?IdentityGeneration $identityGeneration = null,
    bool $isGenerated = false,
    ?string $generationExpression = null,
    ?int $ordinalPosition = null,
): SchemaColumn {
    return SchemaColumn::create(
        $name,
        $type,
        $nullable,
        $default,
        $isIdentity,
        $identityGeneration,
        $isGenerated,
        $generationExpression,
        $ordinalPosition,
    );
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_integer(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::integer(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_smallint(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::smallint(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_bigint(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::bigint(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_serial(string $name): SchemaColumn
{
    return SchemaColumn::create($name, ColumnType::serial(), false);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_small_serial(string $name): SchemaColumn
{
    return SchemaColumn::create($name, ColumnType::smallserial(), false);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_big_serial(string $name): SchemaColumn
{
    return SchemaColumn::create($name, ColumnType::bigserial(), false);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_boolean(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::boolean(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_text(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::text(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_varchar(
    string $name,
    int $length,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::varchar($length), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_char(
    string $name,
    int $length,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::char($length), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_numeric(
    string $name,
    ?int $precision = null,
    ?int $scale = null,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::numeric($precision, $scale), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_real(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::real(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_double_precision(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::doublePrecision(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_date(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::date(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_time(
    string $name,
    ?int $precision = null,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::time($precision), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_timestamp(
    string $name,
    ?int $precision = null,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::timestamp($precision), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_timestamp_tz(
    string $name,
    ?int $precision = null,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::timestamptz($precision), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_interval(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::interval(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_uuid(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::uuid(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_json(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::json(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_jsonb(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::jsonb(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_bytea(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::bytea(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_inet(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::inet(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_cidr(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::cidr(), $nullable, $default);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_column_macaddr(
    string $name,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
): SchemaColumn {
    return SchemaColumn::create($name, ColumnType::macaddr(), $nullable, $default);
}

/**
 * @param non-empty-list<string> $columns
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_primary_key(array $columns, ?string $name = null): SchemaPrimaryKey
{
    return new SchemaPrimaryKey($columns, $name);
}

/**
 * @param non-empty-list<string> $columns
 * @param non-empty-list<string> $referenceColumns
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_foreign_key(
    array $columns,
    string $referenceTable,
    array $referenceColumns,
    ?string $name = null,
    string $referenceSchema = 'public',
    ReferentialAction $onUpdate = ReferentialAction::NO_ACTION,
    ReferentialAction $onDelete = ReferentialAction::NO_ACTION,
    bool $deferrable = false,
    bool $initiallyDeferred = false,
): SchemaForeignKey {
    return new SchemaForeignKey(
        $name,
        $columns,
        $referenceSchema,
        $referenceTable,
        $referenceColumns,
        $onUpdate,
        $onDelete,
        $deferrable,
        $initiallyDeferred,
    );
}

/**
 * @param non-empty-list<string> $columns
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_unique(array $columns, ?string $name = null, bool $nullsNotDistinct = false): SchemaUniqueConstraint
{
    return new SchemaUniqueConstraint($columns, $name, $nullsNotDistinct);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_check(string $expression, ?string $name = null, bool $noInherit = false): SchemaCheckConstraint
{
    return new SchemaCheckConstraint($expression, $name, $noInherit);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_exclude(string $definition, ?string $name = null): SchemaExcludeConstraint
{
    return new SchemaExcludeConstraint($definition, $name);
}

/**
 * @param non-empty-list<string> $columns
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_index(
    string $name,
    array $columns,
    bool $unique = false,
    SchemaIndexMethod $method = SchemaIndexMethod::BTREE,
    bool $primary = false,
    ?string $predicate = null,
): SchemaIndex {
    return new SchemaIndex($name, $columns, $unique, $method, $primary, $predicate);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_sequence(
    string $name,
    string $dataType = 'bigint',
    int|string $startValue = 1,
    int|string $minValue = 1,
    int|string|null $maxValue = null,
    int|string $incrementBy = 1,
    bool $cycle = false,
    int|string $cacheValue = 1,
    ?string $ownedByTable = null,
    ?string $ownedByColumn = null,
): SchemaSequence {
    return new SchemaSequence(
        $name,
        $dataType,
        $startValue,
        $minValue,
        $maxValue,
        $incrementBy,
        $cycle,
        $cacheValue,
        $ownedByTable,
        $ownedByColumn,
    );
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_view(string $name, string $definition, bool $isUpdatable = false): SchemaView
{
    return new SchemaView($name, $definition, $isUpdatable);
}

/**
 * @param list<SchemaIndex> $indexes
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_materialized_view(string $name, string $definition, array $indexes = []): SchemaMaterializedView
{
    return new SchemaMaterializedView($name, $definition, $indexes);
}

/**
 * @param list<string> $argumentTypes
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_function(
    string $name,
    string $returnType,
    array $argumentTypes = [],
    string $language = 'sql',
    ?string $definition = null,
    bool $isStrict = false,
    ?SchemaFunctionVolatility $volatility = null,
): SchemaFunction {
    return new SchemaFunction($name, $returnType, $argumentTypes, $language, $definition, $isStrict, $volatility);
}

/**
 * @param list<string> $argumentTypes
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_procedure(
    string $name,
    array $argumentTypes = [],
    string $language = 'sql',
    ?string $definition = null,
): SchemaProcedure {
    return new SchemaProcedure($name, $argumentTypes, $language, $definition);
}

/**
 * @param non-empty-list<TriggerEvent> $events
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_trigger(
    string $name,
    string $tableName,
    TriggerTiming $timing,
    array $events,
    string $functionName,
    bool $forEachRow = false,
    ?string $whenCondition = null,
): SchemaTrigger {
    return new SchemaTrigger($name, $tableName, $timing, $events, $functionName, $forEachRow, $whenCondition);
}

/**
 * @param list<SchemaCheckConstraint> $checkConstraints
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_domain(
    string $name,
    ColumnType $baseType,
    bool $nullable = true,
    bool|float|int|string|Expression|null $default = null,
    array $checkConstraints = [],
): SchemaDomain {
    return SchemaDomain::create($name, $baseType, $nullable, $default, $checkConstraints);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function schema_extension(string $name, ?string $version = null): SchemaExtension
{
    return new SchemaExtension($name, $version);
}

/**
 * @param ?list<string> $schemaNames
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function client_catalog_provider(
    Client\Client $client,
    ?array $schemaNames = null,
    ?ExclusionPolicy $exclusionPolicy = null,
): CatalogProvider {
    return new PgCatalogProvider($client, $schemaNames, $exclusionPolicy);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function exclude_any(ExclusionPolicy ...$policies): ExclusionPolicy
{
    return new AnyExclusionPolicy(...$policies);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function exclude_exact(string $name): ExclusionPolicy
{
    return new ExactMatchExclusionPolicy($name);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function exclude_starts_with(string $prefix): ExclusionPolicy
{
    return new StartsWithExclusionPolicy($prefix);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function exclude_ends_with(string $suffix): ExclusionPolicy
{
    return new EndsWithExclusionPolicy($suffix);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function exclude_pattern(string $pattern): ExclusionPolicy
{
    return new PatternExclusionPolicy($pattern);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function exclude_schema(string $schema): ExclusionPolicy
{
    return new WholeSchemaExclusionPolicy($schema);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function exclude_scoped(
    ExclusionPolicy $policy,
    ?SchemaObjectType $type = null,
    ?string $schema = null,
): ExclusionPolicy {
    return new ScopedExclusionPolicy($policy, $type, $schema);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function manual_catalog_provider(Catalog $catalog): CatalogProvider
{
    return new ManualCatalogProvider($catalog);
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function chain_catalog_provider(CatalogProvider ...$providers): ChainCatalogProvider
{
    return new ChainCatalogProvider(...$providers);
}

/**
 * @param null|ExecutionOrderStrategy<\Flow\PostgreSql\Schema\Table> $tableOrderStrategy
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function catalog_comparator(
    ?RenameStrategy $renameStrategy = null,
    ?ViewDependencyResolver $viewDependencyResolver = null,
    ?ExecutionOrderStrategy $tableOrderStrategy = null,
    bool $dropIfExists = false,
): CatalogComparator {
    return CatalogComparator::create(
        $renameStrategy,
        $viewDependencyResolver,
        $tableOrderStrategy,
        dropIfExists: $dropIfExists,
    );
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function ast_view_dependency_resolver(): AstViewDependencyResolver
{
    return new AstViewDependencyResolver(new Parser());
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function noop_view_dependency_resolver(): NoopViewDependencyResolver
{
    return new NoopViewDependencyResolver();
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function foreign_key_dependency_order(): ForeignKeyDependencyOrder
{
    return new ForeignKeyDependencyOrder();
}

/**
 * @return NoExecutionOrder<mixed>
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function no_execution_order(): NoExecutionOrder
{
    return new NoExecutionOrder();
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function view_dependency_order(): ViewDependencyOrder
{
    return new ViewDependencyOrder(new Parser());
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function materialized_view_dependency_order(): MaterializedViewDependencyOrder
{
    return new MaterializedViewDependencyOrder(new Parser());
}
