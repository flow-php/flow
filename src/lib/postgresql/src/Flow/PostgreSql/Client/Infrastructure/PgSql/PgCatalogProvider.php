<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Infrastructure\PgSql;

use function Flow\PostgreSql\DSL\{agg, and_, any_, asc, case_when, cast, col, concat, eq, func, gt, in_, is_null, is_true, literal, ne, not_, not_like, param, select, sub_select, table, type_mapper, when};

use function Flow\Types\DSL\{type_boolean, type_integer, type_null, type_string, type_structure, type_union};
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Parser\{ColumnTypeParser, ExpressionParser};
use Flow\PostgreSql\QueryBuilder\Condition\ComparisonOperator;

use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use Flow\PostgreSql\QueryBuilder\Schema\{ColumnType, ReferentialAction};
use Flow\PostgreSql\Schema\{Catalog, CatalogProvider, Column, Domain, Extension, Func, FunctionVolatility, IdentityGeneration, Index, IndexMethod, MaterializedView, PartitionStrategy, Procedure, Schema, Sequence, Table, Trigger, TriggerEvent, TriggerTiming, View};
use Flow\PostgreSql\Schema\Constraint\{CheckConstraint, ExcludeConstraint, ForeignKey, PrimaryKey, UniqueConstraint};

final readonly class PgCatalogProvider implements CatalogProvider
{
    private ColumnTypeParser $columnTypeParser;

    private ExpressionParser $expressionParser;

    /**
     * @param ?list<string> $schemaNames
     * @param list<string> $excludeTables
     */
    public function __construct(
        private Client $client,
        private ?array $schemaNames = null,
        private array $excludeTables = [],
    ) {
        $this->columnTypeParser = new ColumnTypeParser();
        $this->expressionParser = new ExpressionParser(new Parser());
    }

    public function get() : Catalog
    {
        $schemas = [];

        foreach ($this->resolveSchemaNames() as $schemaName) {
            $schemas[] = $this->readSchema($schemaName);
        }

        return new Catalog($schemas);
    }

    private function mapIndexMethod(string $amname) : IndexMethod
    {
        return IndexMethod::tryFrom($amname) ?? IndexMethod::BTREE;
    }

    private function mapReferentialAction(string $code) : ReferentialAction
    {
        return ReferentialAction::tryFrom($code) ?? ReferentialAction::NO_ACTION;
    }

    /**
     * Strip implicit type casts from default values that PostgreSQL adds for storage.
     *
     * PostgreSQL stores `'pending'` as `'pending'::character varying` — the cast is implicit
     * and redundant since the column type is already known. This method parses the expression
     * and removes the outer TypeCast when it wraps a simple constant.
     */
    private function normalizeDefault(?string $default) : ?string
    {
        if ($default === null) {
            return null;
        }

        $node = $this->expressionParser->parse($default);
        $typeCast = $node->getTypeCast();

        if ($typeCast === null) {
            return $default;
        }

        $inner = $typeCast->getArg();

        if ($inner === null || $inner->getAConst() === null) {
            return $default;
        }

        $aConst = $inner->getAConst();
        $sval = $aConst->getSval();

        if ($sval !== null) {
            return "'" . \str_replace("'", "''", $sval->getSval()) . "'";
        }

        $ival = $aConst->getIval();

        if ($ival !== null) {
            /** @phpstan-ignore method.nonObject */
            return (string) $ival->getIval();
        }

        $fval = $aConst->getFval();

        if ($fval !== null) {
            return $fval->getFval();
        }

        return $default;
    }

    /**
     * @return list<string>
     */
    private function parseArgumentTypes(string $arguments) : array
    {
        if ($arguments === '') {
            return [];
        }

        $types = [];

        foreach (\explode(', ', $arguments) as $arg) {
            $parts = \explode(' ', \trim($arg));
            $types[] = \end($parts);
        }

        return $types;
    }

    /**
     * @return non-empty-list<string>
     */
    private function parseArrayLiteral(string $literal) : array
    {
        $trimmed = \trim($literal, '{}');

        if ($trimmed === '') {
            throw new \RuntimeException(\sprintf('Expected non-empty array literal, got "%s".', $literal));
        }

        return \array_values(\array_map('trim', \explode(',', $trimmed)));
    }

    /**
     * @return list<CheckConstraint>
     */
    private function readCheckConstraints(string $tableName, string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'definition' => type_string(),
                'no_inherit' => type_boolean(),
            ])),
            select(
                col('conname', 'con')->as('name'),
                func('pg_catalog.pg_get_constraintdef', [col('oid', 'con')])->as('definition'),
                col('connoinherit', 'con')->as('no_inherit'),
            )
                ->from(table('pg_constraint', 'pg_catalog')->as('con'))
                ->join(table('pg_class', 'pg_catalog')->as('c'), eq(col('oid', 'c'), col('conrelid', 'con')))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('relnamespace', 'c')))
                ->where(and_(
                    eq(col('relname', 'c'), param(1)),
                    eq(col('nspname', 'n'), param(2)),
                    eq(col('contype', 'con'), literal('c')),
                ))
                ->orderBy(asc(col('conname', 'con'))),
            [$tableName, $schemaName],
        );

        $constraints = [];

        foreach ($rows as $row) {
            $constraints[] = new CheckConstraint(
                $this->stripCheckWrapper($row['definition']),
                $row['name'],
                $row['no_inherit'],
            );
        }

        return $constraints;
    }

    /**
     * @return non-empty-list<Column>
     */
    private function readColumns(string $tableName, string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'type_name' => type_string(),
                'nullable' => type_boolean(),
                'identity' => type_string(),
                'generated' => type_string(),
                'ordinal_position' => type_integer(),
            ], [
                'default_value' => type_union(type_string(), type_null()),
            ])),
            select(
                col('attname', 'a')->as('name'),
                func('pg_catalog.format_type', [col('atttypid', 'a'), col('atttypmod', 'a')])->as('type_name'),
                not_(col('attnotnull', 'a'))->as('nullable'),
                func('pg_catalog.pg_get_expr', [col('adbin', 'd'), col('adrelid', 'd')])->as('default_value'),
                col('attidentity', 'a')->as('identity'),
                col('attgenerated', 'a')->as('generated'),
                col('attnum', 'a')->as('ordinal_position'),
            )
                ->from(table('pg_attribute', 'pg_catalog')->as('a'))
                ->leftJoin(
                    table('pg_attrdef', 'pg_catalog')->as('d'),
                    and_(
                        eq(col('adrelid', 'd'), col('attrelid', 'a')),
                        eq(col('adnum', 'd'), col('attnum', 'a')),
                    ),
                )
                ->where(and_(
                    eq(
                        col('attrelid', 'a'),
                        sub_select(
                            select(col('oid', 'c'))
                                ->from(table('pg_class', 'pg_catalog')->as('c'))
                                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('relnamespace', 'c')))
                                ->where(and_(
                                    eq(col('relname', 'c'), param(1)),
                                    eq(col('nspname', 'n'), param(2)),
                                ))
                        ),
                    ),
                    gt(col('attnum', 'a'), literal(0)),
                    not_(is_true(col('attisdropped', 'a'))),
                ))
                ->orderBy(asc(col('attnum', 'a'))),
            [$tableName, $schemaName],
        );

        $columns = [];

        foreach ($rows as $row) {
            $isIdentity = $row['identity'] !== '';
            $isGenerated = $row['generated'] !== '';

            $defaultValue = $row['default_value'] ?? null;

            $columns[] = new Column(
                $row['name'],
                $this->columnTypeParser->parse($row['type_name']),
                $row['nullable'],
                $isGenerated || $isIdentity ? null : $this->normalizeDefault($defaultValue),
                $isIdentity,
                $isIdentity ? IdentityGeneration::from($row['identity']) : null,
                $isGenerated,
                $isGenerated && $defaultValue !== null ? $this->expressionParser->normalize($defaultValue) : null,
                $row['ordinal_position'],
            );
        }

        if ($columns === []) {
            throw new \RuntimeException(\sprintf('Table "%s"."%s" has no columns.', $schemaName, $tableName));
        }

        return $columns;
    }

    /**
     * @return list<CheckConstraint>
     */
    private function readDomainCheckConstraints(string $domainName, string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'definition' => type_string(),
            ])),
            select(
                col('conname', 'con')->as('name'),
                func('pg_catalog.pg_get_constraintdef', [col('oid', 'con')])->as('definition'),
            )
                ->from(table('pg_constraint', 'pg_catalog')->as('con'))
                ->join(table('pg_type', 'pg_catalog')->as('t'), eq(col('oid', 't'), col('contypid', 'con')))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('typnamespace', 't')))
                ->where(and_(
                    eq(col('typname', 't'), param(1)),
                    eq(col('nspname', 'n'), param(2)),
                    eq(col('contype', 'con'), literal('c')),
                ))
                ->orderBy(asc(col('conname', 'con'))),
            [$domainName, $schemaName],
        );

        $constraints = [];

        foreach ($rows as $row) {
            $constraints[] = new CheckConstraint(
                $this->stripCheckWrapper($row['definition']),
                $row['name'],
            );
        }

        return $constraints;
    }

    /**
     * @return list<Domain>
     */
    private function readDomains(string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'base_type' => type_string(),
                'nullable' => type_boolean(),
            ], [
                'default_value' => type_union(type_string(), type_null()),
            ])),
            select(
                col('typname', 't')->as('name'),
                func('pg_catalog.format_type', [col('typbasetype', 't'), col('typtypmod', 't')])->as('base_type'),
                not_(col('typnotnull', 't'))->as('nullable'),
                func('pg_catalog.pg_get_expr', [col('typdefaultbin', 't'), literal(0)])->as('default_value'),
            )
                ->from(table('pg_type', 'pg_catalog')->as('t'))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('typnamespace', 't')))
                ->where(and_(
                    eq(col('nspname', 'n'), param(1)),
                    eq(col('typtype', 't'), literal('d')),
                ))
                ->orderBy(asc(col('typname', 't'))),
            [$schemaName],
        );

        $domains = [];

        foreach ($rows as $row) {
            $domains[] = new Domain(
                $row['name'],
                $this->columnTypeParser->parse($row['base_type']),
                $row['nullable'],
                $row['default_value'] ?? null,
                $this->readDomainCheckConstraints($row['name'], $schemaName),
            );
        }

        return $domains;
    }

    /**
     * @return list<ExcludeConstraint>
     */
    private function readExcludeConstraints(string $tableName, string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'definition' => type_string(),
            ])),
            select(
                col('conname', 'con')->as('name'),
                func('pg_catalog.pg_get_constraintdef', [col('oid', 'con')])->as('definition'),
            )
                ->from(table('pg_constraint', 'pg_catalog')->as('con'))
                ->join(table('pg_class', 'pg_catalog')->as('c'), eq(col('oid', 'c'), col('conrelid', 'con')))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('relnamespace', 'c')))
                ->where(and_(
                    eq(col('relname', 'c'), param(1)),
                    eq(col('nspname', 'n'), param(2)),
                    eq(col('contype', 'con'), literal('x')),
                ))
                ->orderBy(asc(col('conname', 'con'))),
            [$tableName, $schemaName],
        );

        $constraints = [];

        foreach ($rows as $row) {
            $constraints[] = new ExcludeConstraint($row['definition'], $row['name']);
        }

        return $constraints;
    }

    /**
     * @return list<Extension>
     */
    private function readExtensions(string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
            ], [
                'version' => type_union(type_string(), type_null()),
            ])),
            select(
                col('extname', 'e')->as('name'),
                col('extversion', 'e')->as('version'),
            )
                ->from(table('pg_extension', 'pg_catalog')->as('e'))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('extnamespace', 'e')))
                ->where(eq(col('nspname', 'n'), param(1)))
                ->orderBy(asc(col('extname', 'e'))),
            [$schemaName],
        );

        $extensions = [];

        foreach ($rows as $row) {
            $extensions[] = new Extension($row['name'], $row['version'] ?? null);
        }

        return $extensions;
    }

    /**
     * @return list<ForeignKey>
     */
    private function readForeignKeys(string $tableName, string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'columns' => type_string(),
                'reference_schema' => type_string(),
                'reference_table' => type_string(),
                'reference_columns' => type_string(),
                'on_update' => type_string(),
                'on_delete' => type_string(),
                'deferrable' => type_boolean(),
                'initially_deferred' => type_boolean(),
            ])),
            select(
                col('conname', 'con')->as('name'),
                agg('array_agg', [col('attname', 'a')], distinct: true)->withOrderBy(asc(col('attname', 'a')))->as('columns'),
                col('nspname', 'nf')->as('reference_schema'),
                col('relname', 'cf')->as('reference_table'),
                agg('array_agg', [col('attname', 'af')], distinct: true)->withOrderBy(asc(col('attname', 'af')))->as('reference_columns'),
                col('confupdtype', 'con')->as('on_update'),
                col('confdeltype', 'con')->as('on_delete'),
                col('condeferrable', 'con')->as('deferrable'),
                col('condeferred', 'con')->as('initially_deferred'),
            )
                ->from(table('pg_constraint', 'pg_catalog')->as('con'))
                ->join(table('pg_class', 'pg_catalog')->as('c'), eq(col('oid', 'c'), col('conrelid', 'con')))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('relnamespace', 'c')))
                ->join(table('pg_attribute', 'pg_catalog')->as('a'), and_(
                    eq(col('attrelid', 'a'), col('conrelid', 'con')),
                    any_(col('attnum', 'a'), ComparisonOperator::EQ, col('conkey', 'con')),
                ))
                ->join(table('pg_class', 'pg_catalog')->as('cf'), eq(col('oid', 'cf'), col('confrelid', 'con')))
                ->join(table('pg_namespace', 'pg_catalog')->as('nf'), eq(col('oid', 'nf'), col('relnamespace', 'cf')))
                ->join(table('pg_attribute', 'pg_catalog')->as('af'), and_(
                    eq(col('attrelid', 'af'), col('confrelid', 'con')),
                    any_(col('attnum', 'af'), ComparisonOperator::EQ, col('confkey', 'con')),
                ))
                ->where(and_(
                    eq(col('relname', 'c'), param(1)),
                    eq(col('nspname', 'n'), param(2)),
                    eq(col('contype', 'con'), literal('f')),
                ))
                ->groupBy(
                    col('conname', 'con'),
                    col('nspname', 'nf'),
                    col('relname', 'cf'),
                    col('confupdtype', 'con'),
                    col('confdeltype', 'con'),
                    col('condeferrable', 'con'),
                    col('condeferred', 'con'),
                ),
            [$tableName, $schemaName],
        );

        $foreignKeys = [];

        foreach ($rows as $row) {
            $foreignKeys[] = new ForeignKey(
                $row['name'],
                $this->parseArrayLiteral($row['columns']),
                $row['reference_schema'],
                $row['reference_table'],
                $this->parseArrayLiteral($row['reference_columns']),
                $this->mapReferentialAction($row['on_update']),
                $this->mapReferentialAction($row['on_delete']),
                $row['deferrable'],
                $row['initially_deferred'],
            );
        }

        return $foreignKeys;
    }

    /**
     * @return list<Func>
     */
    private function readFunctions(string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'return_type' => type_string(),
                'arguments' => type_string(),
                'language' => type_string(),
                'definition' => type_string(),
                'is_strict' => type_boolean(),
            ], [
                'volatility' => type_union(type_string(), type_null()),
            ])),
            select(
                col('proname', 'p')->as('name'),
                func('pg_catalog.pg_get_function_result', [col('oid', 'p')])->as('return_type'),
                func('pg_catalog.pg_get_function_arguments', [col('oid', 'p')])->as('arguments'),
                col('lanname', 'l')->as('language'),
                col('prosrc', 'p')->as('definition'),
                col('proisstrict', 'p')->as('is_strict'),
                case_when(
                    [when(literal('i'), literal('immutable')), when(literal('s'), literal('stable')), when(literal('v'), literal('volatile'))],
                    operand: col('provolatile', 'p'),
                )->as('volatility'),
            )
                ->from(table('pg_proc', 'pg_catalog')->as('p'))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('pronamespace', 'p')))
                ->join(table('pg_language', 'pg_catalog')->as('l'), eq(col('oid', 'l'), col('prolang', 'p')))
                ->where(and_(
                    eq(col('nspname', 'n'), param(1)),
                    eq(col('prokind', 'p'), literal('f')),
                ))
                ->orderBy(asc(col('proname', 'p'))),
            [$schemaName],
        );

        $functions = [];

        foreach ($rows as $row) {
            $functions[] = new Func(
                $row['name'],
                $row['return_type'],
                $this->parseArgumentTypes($row['arguments']),
                $row['language'],
                $row['definition'],
                $row['is_strict'],
                array_key_exists('volatility', $row) && $row['volatility'] !== null ? FunctionVolatility::from($row['volatility']) : null,
            );
        }

        return $functions;
    }

    /**
     * @return list<Index>
     */
    private function readIndexes(string $tableName, string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'is_unique' => type_boolean(),
                'is_primary' => type_boolean(),
                'method' => type_string(),
                'columns' => type_string(),
            ], [
                'predicate' => type_union(type_string(), type_null()),
            ])),
            select(
                col('relname', 'i')->as('name'),
                col('indisunique', 'ix')->as('is_unique'),
                col('indisprimary', 'ix')->as('is_primary'),
                col('amname', 'am')->as('method'),
                agg('array_agg', [col('attname', 'a')])->withOrderBy(asc(func('array_position', [cast(col('indkey', 'ix'), ColumnType::array(ColumnType::integer())), col('attnum', 'a')])))->as('columns'),
                func('pg_catalog.pg_get_expr', [col('indpred', 'ix'), col('indrelid', 'ix')])->as('predicate'),
            )
                ->from(table('pg_index', 'pg_catalog')->as('ix'))
                ->join(table('pg_class', 'pg_catalog')->as('i'), eq(col('oid', 'i'), col('indexrelid', 'ix')))
                ->join(table('pg_class', 'pg_catalog')->as('t'), eq(col('oid', 't'), col('indrelid', 'ix')))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('relnamespace', 't')))
                ->join(table('pg_am', 'pg_catalog')->as('am'), eq(col('oid', 'am'), col('relam', 'i')))
                ->join(table('pg_attribute', 'pg_catalog')->as('a'), and_(
                    eq(col('attrelid', 'a'), col('oid', 't')),
                    any_(col('attnum', 'a'), ComparisonOperator::EQ, col('indkey', 'ix')),
                ))
                ->leftJoin(
                    table('pg_constraint', 'pg_catalog')->as('con'),
                    eq(col('conindid', 'con'), col('indexrelid', 'ix')),
                )
                ->where(and_(
                    eq(col('relname', 't'), param(1)),
                    eq(col('nspname', 'n'), param(2)),
                    gt(col('attnum', 'a'), literal(0)),
                    is_null(col('conindid', 'con')),
                ))
                ->groupBy(
                    col('relname', 'i'),
                    col('indisunique', 'ix'),
                    col('indisprimary', 'ix'),
                    col('amname', 'am'),
                    col('indpred', 'ix'),
                    col('indrelid', 'ix'),
                )
                ->orderBy(asc(col('relname', 'i'))),
            [$tableName, $schemaName],
        );

        $indexes = [];

        foreach ($rows as $row) {
            $indexes[] = new Index(
                $row['name'],
                $this->parseArrayLiteral($row['columns']),
                $row['is_unique'],
                $this->mapIndexMethod($row['method']),
                $row['is_primary'],
                $row['predicate'] ?? null,
            );
        }

        return $indexes;
    }

    /**
     * @return list<string>
     */
    private function readInherits(string $tableName, string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure(['parent' => type_string()])),
            select(
                concat(col('nspname', 'pn'), literal('.'), col('relname', 'pc'))->as('parent'),
            )
                ->from(table('pg_inherits', 'pg_catalog')->as('i'))
                ->join(table('pg_class', 'pg_catalog')->as('c'), eq(col('oid', 'c'), col('inhrelid', 'i')))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('relnamespace', 'c')))
                ->join(table('pg_class', 'pg_catalog')->as('pc'), eq(col('oid', 'pc'), col('inhparent', 'i')))
                ->join(table('pg_namespace', 'pg_catalog')->as('pn'), eq(col('oid', 'pn'), col('relnamespace', 'pc')))
                ->where(and_(
                    eq(col('relname', 'c'), param(1)),
                    eq(col('nspname', 'n'), param(2)),
                ))
                ->orderBy(asc(col('inhseqno', 'i'))),
            [$tableName, $schemaName],
        );

        return \array_map(static fn (array $row) : string => $row['parent'], $rows);
    }

    /**
     * @return list<MaterializedView>
     */
    private function readMaterializedViews(string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'definition' => type_string(),
            ])),
            select(
                col('matviewname')->as('name'),
                col('definition'),
            )
                ->from(table('pg_matviews', 'pg_catalog'))
                ->where(eq(col('schemaname'), param(1)))
                ->orderBy(asc(col('matviewname'))),
            [$schemaName],
        );

        $views = [];

        foreach ($rows as $row) {
            $views[] = new MaterializedView($row['name'], $row['definition']);
        }

        return $views;
    }

    /**
     * @return array{0: PartitionStrategy, 1: list<string>}
     */
    private function readPartitionInfo(string $tableName, string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure(['partdef' => type_string()])),
            select(
                func('pg_catalog.pg_get_partkeydef', [col('oid', 'c')])->as('partdef'),
            )
                ->from(table('pg_class', 'pg_catalog')->as('c'))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('relnamespace', 'c')))
                ->where(and_(
                    eq(col('relname', 'c'), param(1)),
                    eq(col('nspname', 'n'), param(2)),
                )),
            [$tableName, $schemaName],
        );

        if ($rows === [] || $rows[0]['partdef'] === '') {
            throw new \RuntimeException(\sprintf('Could not read partition info for table "%s"."%s".', $schemaName, $tableName));
        }

        $partdef = $rows[0]['partdef'];

        if (!\preg_match('/^(HASH|LIST|RANGE)\s*\((.+)\)$/i', $partdef, $matches)) {
            throw new \RuntimeException(\sprintf('Could not parse partition definition: "%s".', $partdef));
        }

        return [
            PartitionStrategy::from(\strtolower($matches[1])),
            \array_map('trim', \explode(',', $matches[2])),
        ];
    }

    private function readPrimaryKey(string $tableName, string $schemaName) : ?PrimaryKey
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'columns' => type_string(),
            ])),
            select(
                col('conname', 'con')->as('name'),
                agg('array_agg', [col('attname', 'a')])->withOrderBy(asc(func('array_position', [col('conkey', 'con'), col('attnum', 'a')])))->as('columns'),
            )
                ->from(table('pg_constraint', 'pg_catalog')->as('con'))
                ->join(table('pg_class', 'pg_catalog')->as('c'), eq(col('oid', 'c'), col('conrelid', 'con')))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('relnamespace', 'c')))
                ->join(table('pg_attribute', 'pg_catalog')->as('a'), and_(
                    eq(col('attrelid', 'a'), col('conrelid', 'con')),
                    any_(col('attnum', 'a'), ComparisonOperator::EQ, col('conkey', 'con')),
                ))
                ->where(and_(
                    eq(col('relname', 'c'), param(1)),
                    eq(col('nspname', 'n'), param(2)),
                    eq(col('contype', 'con'), literal('p')),
                ))
                ->groupBy(col('conname', 'con')),
            [$tableName, $schemaName],
        );

        if ($rows === []) {
            return null;
        }

        return new PrimaryKey(
            $this->parseArrayLiteral($rows[0]['columns']),
            $rows[0]['name'],
        );
    }

    /**
     * @return list<Procedure>
     */
    private function readProcedures(string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'arguments' => type_string(),
                'language' => type_string(),
            ], [
                'definition' => type_union(type_string(), type_null()),
            ])),
            select(
                col('proname', 'p')->as('name'),
                func('pg_catalog.pg_get_function_arguments', [col('oid', 'p')])->as('arguments'),
                col('lanname', 'l')->as('language'),
                col('prosrc', 'p')->as('definition'),
            )
                ->from(table('pg_proc', 'pg_catalog')->as('p'))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('pronamespace', 'p')))
                ->join(table('pg_language', 'pg_catalog')->as('l'), eq(col('oid', 'l'), col('prolang', 'p')))
                ->where(and_(
                    eq(col('nspname', 'n'), param(1)),
                    eq(col('prokind', 'p'), literal('p')),
                ))
                ->orderBy(asc(col('proname', 'p'))),
            [$schemaName],
        );

        $procedures = [];

        foreach ($rows as $row) {
            $procedures[] = new Procedure(
                $row['name'],
                $this->parseArgumentTypes($row['arguments']),
                $row['language'],
                $row['definition'] ?? null,
            );
        }

        return $procedures;
    }

    private function readSchema(string $schemaName) : Schema
    {
        $tables = [];

        foreach ($this->readTableNames($schemaName) as $tableInfo) {
            $tables[] = $this->readTable(
                $tableInfo['relname'],
                $schemaName,
                $tableInfo['relpersistence'] === 'u',
                $tableInfo['relkind'],
                $tableInfo['tablespace'] ?? null,
            );
        }

        return new Schema(
            $schemaName,
            $tables,
            $this->readSequences($schemaName),
            $this->readViews($schemaName),
            $this->readMaterializedViews($schemaName),
            $this->readFunctions($schemaName),
            $this->readProcedures($schemaName),
            $this->readDomains($schemaName),
            $this->readExtensions($schemaName),
        );
    }

    /**
     * @return list<Sequence>
     */
    private function readSequences(string $schemaName) : array
    {
        $intOrString = type_union(type_integer(), type_string());

        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'data_type' => type_string(),
                'start_value' => $intOrString,
                'min_value' => $intOrString,
                'increment_by' => $intOrString,
                'cycle' => type_boolean(),
                'cache_value' => $intOrString,
            ], [
                'max_value' => type_union(type_integer(), type_string(), type_null()),
                'owned_by_table' => type_union(type_string(), type_null()),
                'owned_by_column' => type_union(type_string(), type_null()),
            ])),
            select(
                col('relname', 'c')->as('name'),
                func('pg_catalog.format_type', [col('seqtypid', 's'), literal(null)])->as('data_type'),
                col('seqstart', 's')->as('start_value'),
                col('seqmin', 's')->as('min_value'),
                col('seqmax', 's')->as('max_value'),
                col('seqincrement', 's')->as('increment_by'),
                col('seqcycle', 's')->as('cycle'),
                col('seqcache', 's')->as('cache_value'),
                col('relname', 'dep_c')->as('owned_by_table'),
                col('attname', 'dep_a')->as('owned_by_column'),
            )
                ->from(table('pg_sequence', 'pg_catalog')->as('s'))
                ->join(table('pg_class', 'pg_catalog')->as('c'), eq(col('oid', 'c'), col('seqrelid', 's')))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('relnamespace', 'c')))
                ->leftJoin(table('pg_depend', 'pg_catalog')->as('d'), and_(
                    eq(col('objid', 'd'), col('seqrelid', 's')),
                    in_(col('deptype', 'd'), [literal('a'), literal('i')]),
                ))
                ->leftJoin(table('pg_class', 'pg_catalog')->as('dep_c'), eq(col('oid', 'dep_c'), col('refobjid', 'd')))
                ->leftJoin(table('pg_attribute', 'pg_catalog')->as('dep_a'), and_(
                    eq(col('attrelid', 'dep_a'), col('refobjid', 'd')),
                    eq(col('attnum', 'dep_a'), col('refobjsubid', 'd')),
                ))
                ->where(eq(col('nspname', 'n'), param(1)))
                ->orderBy(asc(col('relname', 'c'))),
            [$schemaName],
        );

        $sequences = [];

        foreach ($rows as $row) {
            if (($row['owned_by_table'] ?? null) !== null) {
                continue;
            }

            $sequences[] = new Sequence(
                $row['name'],
                $row['data_type'],
                $row['start_value'],
                $row['min_value'],
                $row['max_value'] ?? null,
                $row['increment_by'],
                $row['cycle'],
                $row['cache_value'],
                null,
                null,
            );
        }

        return $sequences;
    }

    private function readTable(string $tableName, string $schemaName, bool $unlogged = false, string $relkind = 'r', ?string $tablespace = null) : Table
    {
        $partitionStrategy = null;
        $partitionColumns = [];

        if ($relkind === 'p') {
            [$partitionStrategy, $partitionColumns] = $this->readPartitionInfo($tableName, $schemaName);
        }

        $inherits = $relkind === 'r' ? $this->readInherits($tableName, $schemaName) : [];

        return new Table(
            $schemaName,
            $tableName,
            $this->readColumns($tableName, $schemaName),
            $this->readPrimaryKey($tableName, $schemaName),
            $this->readIndexes($tableName, $schemaName),
            $this->readForeignKeys($tableName, $schemaName),
            $this->readUniqueConstraints($tableName, $schemaName),
            $this->readCheckConstraints($tableName, $schemaName),
            $this->readExcludeConstraints($tableName, $schemaName),
            $this->readTriggers($tableName, $schemaName),
            $unlogged,
            $partitionStrategy,
            $partitionColumns,
            $inherits,
            $tablespace,
        );
    }

    /**
     * @return list<array{relname: string, relpersistence: string, relkind: string, tablespace?: ?string}>
     */
    private function readTableNames(string $schemaName) : array
    {
        $conditions = and_(
            eq(col('nspname', 'n'), param(1)),
            in_(col('relkind', 'c'), [literal('r'), literal('p')]),
        );

        if ($this->excludeTables !== []) {
            $conditions = and_(
                $conditions,
                not_(in_(col('relname', 'c'), \array_map(static fn (string $t) : Literal => literal($t), $this->excludeTables))),
            );
        }

        return \array_values($this->client->fetchAllInto(
            type_mapper(type_structure([
                'relname' => type_string(),
                'relpersistence' => type_string(),
                'relkind' => type_string(),
            ], [
                'tablespace' => type_union(type_string(), type_null()),
            ])),
            select(
                col('relname', 'c'),
                col('relpersistence', 'c'),
                col('relkind', 'c'),
                case_when(
                    [when(eq(col('reltablespace', 'c'), literal(0)), Literal::null())],
                    elseResult: col('spcname', 'ts'),
                )->as('tablespace'),
            )
                ->from(table('pg_class', 'pg_catalog')->as('c'))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('relnamespace', 'c')))
                ->leftJoin(table('pg_tablespace', 'pg_catalog')->as('ts'), eq(col('oid', 'ts'), col('reltablespace', 'c')))
                ->where($conditions)
                ->orderBy(asc(col('relname', 'c'))),
            [$schemaName],
        ));
    }

    /**
     * @return list<Trigger>
     */
    private function readTriggers(string $tableName, string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'function_name' => type_string(),
                'type' => type_integer(),
            ])),
            select(
                col('tgname', 't')->as('name'),
                col('proname', 'p')->as('function_name'),
                col('tgtype', 't')->as('type'),
            )
                ->from(table('pg_trigger', 'pg_catalog')->as('t'))
                ->join(table('pg_class', 'pg_catalog')->as('c'), eq(col('oid', 'c'), col('tgrelid', 't')))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('relnamespace', 'c')))
                ->join(table('pg_proc', 'pg_catalog')->as('p'), eq(col('oid', 'p'), col('tgfoid', 't')))
                ->where(and_(
                    eq(col('relname', 'c'), param(1)),
                    eq(col('nspname', 'n'), param(2)),
                    not_(is_true(col('tgisinternal', 't'))),
                ))
                ->orderBy(asc(col('tgname', 't'))),
            [$tableName, $schemaName],
        );

        $triggers = [];

        foreach ($rows as $row) {
            $tgtype = $row['type'];

            if ($tgtype & 64) {
                $timing = TriggerTiming::INSTEAD_OF;
            } elseif ($tgtype & 2) {
                $timing = TriggerTiming::BEFORE;
            } else {
                $timing = TriggerTiming::AFTER;
            }

            $events = [];

            if ($tgtype & 4) {
                $events[] = TriggerEvent::INSERT;
            }

            if ($tgtype & 8) {
                $events[] = TriggerEvent::DELETE;
            }

            if ($tgtype & 16) {
                $events[] = TriggerEvent::UPDATE;
            }

            if ($tgtype & 32) {
                $events[] = TriggerEvent::TRUNCATE;
            }

            if ($events === []) {
                continue;
            }

            $triggers[] = new Trigger(
                $row['name'],
                $tableName,
                $timing,
                $events,
                $row['function_name'],
                ($tgtype & 1) !== 0,
            );
        }

        return $triggers;
    }

    /**
     * @return list<UniqueConstraint>
     */
    private function readUniqueConstraints(string $tableName, string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'columns' => type_string(),
            ])),
            select(
                col('conname', 'con')->as('name'),
                agg('array_agg', [col('attname', 'a')])->withOrderBy(asc(func('array_position', [col('conkey', 'con'), col('attnum', 'a')])))->as('columns'),
            )
                ->from(table('pg_constraint', 'pg_catalog')->as('con'))
                ->join(table('pg_class', 'pg_catalog')->as('c'), eq(col('oid', 'c'), col('conrelid', 'con')))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('relnamespace', 'c')))
                ->join(table('pg_attribute', 'pg_catalog')->as('a'), and_(
                    eq(col('attrelid', 'a'), col('conrelid', 'con')),
                    any_(col('attnum', 'a'), ComparisonOperator::EQ, col('conkey', 'con')),
                ))
                ->where(and_(
                    eq(col('relname', 'c'), param(1)),
                    eq(col('nspname', 'n'), param(2)),
                    eq(col('contype', 'con'), literal('u')),
                ))
                ->groupBy(col('conname', 'con')),
            [$tableName, $schemaName],
        );

        $constraints = [];

        foreach ($rows as $row) {
            $constraints[] = new UniqueConstraint(
                $this->parseArrayLiteral($row['columns']),
                $row['name'],
            );
        }

        return $constraints;
    }

    /**
     * @return list<View>
     */
    private function readViews(string $schemaName) : array
    {
        $rows = $this->client->fetchAllInto(
            type_mapper(type_structure([
                'name' => type_string(),
                'definition' => type_string(),
            ])),
            select(
                col('viewname')->as('name'),
                col('definition'),
            )
                ->from(table('pg_views', 'pg_catalog'))
                ->where(eq(col('schemaname'), param(1)))
                ->orderBy(asc(col('viewname'))),
            [$schemaName],
        );

        $views = [];

        foreach ($rows as $row) {
            $views[] = new View($row['name'], $row['definition']);
        }

        return $views;
    }

    /**
     * @return list<string>
     */
    private function resolveSchemaNames() : array
    {
        return $this->schemaNames ?? \array_values(\array_map(
            static fn (array $row) : string => $row['nspname'],
            $this->client->fetchAllInto(
                type_mapper(type_structure(['nspname' => type_string()])),
                select(col('nspname'))
                    ->from(table('pg_namespace', 'pg_catalog'))
                    ->where(and_(
                        not_like(col('nspname'), literal('pg_%')),
                        ne(col('nspname'), literal('information_schema')),
                    ))
                    ->orderBy(asc(col('nspname'))),
            ),
        ));
    }

    private function stripCheckWrapper(string $definition) : string
    {
        if (\str_starts_with($definition, 'CHECK (') && \str_ends_with($definition, ')')) {
            return \substr($definition, 7, -1);
        }

        return $definition;
    }
}
