<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Exception\SchemaException;
use Flow\PostgreSql\Schema\Exception\TableNotFoundException;

use function array_map;
use function array_values;
use function sprintf;

/**
 * @import-type TableShape from Table
 * @import-type SequenceShape from Sequence
 * @import-type ViewShape from View
 * @import-type MaterializedViewShape from MaterializedView
 * @import-type FuncShape from Func
 * @import-type ProcedureShape from Procedure
 * @import-type DomainShape from Domain
 *
 * @type SchemaShape = array{name: string, tables?: list<TableShape>, sequences?: list<SequenceShape>, views?: list<ViewShape>, materialized_views?: list<MaterializedViewShape>, functions?: list<FuncShape>, procedures?: list<ProcedureShape>, domains?: list<DomainShape>}
 */
final readonly class Schema
{
    /**
     * @var list<Domain>
     */
    public array $domains;

    /**
     * @var list<Func>
     */
    public array $functions;

    /**
     * @var list<MaterializedView>
     */
    public array $materializedViews;

    /**
     * @var list<Procedure>
     */
    public array $procedures;

    /**
     * @var list<Sequence>
     */
    public array $sequences;

    /**
     * @var list<Table>
     */
    public array $tables;

    /**
     * @var list<View>
     */
    public array $views;

    /**
     * @param list<Table> $tables
     * @param list<Sequence> $sequences
     * @param list<View> $views
     * @param list<MaterializedView> $materializedViews
     * @param list<Func> $functions
     * @param list<Procedure> $procedures
     * @param list<Domain> $domains
     */
    public function __construct(
        public string $name,
        array $tables = [],
        array $sequences = [],
        array $views = [],
        array $materializedViews = [],
        array $functions = [],
        array $procedures = [],
        array $domains = [],
    ) {
        $this->tables = self::bind($tables, $name);
        $this->sequences = self::bind($sequences, $name);
        $this->views = self::bind($views, $name);
        $this->materializedViews = self::bind($materializedViews, $name);
        $this->functions = self::bind($functions, $name);
        $this->procedures = self::bind($procedures, $name);
        $this->domains = self::bind($domains, $name);
    }

    /**
     * @param SchemaShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: $data['name'],
            tables: array_map(static fn(array $t): Table => Table::fromArray($t), $data['tables'] ?? []),
            sequences: array_map(static fn(array $s): Sequence => Sequence::fromArray($s), $data['sequences'] ?? []),
            views: array_map(static fn(array $v): View => View::fromArray($v), $data['views'] ?? []),
            materializedViews: array_map(static fn(array $mv): MaterializedView => MaterializedView::fromArray(
                $mv,
            ), $data['materialized_views'] ?? []),
            functions: array_map(static fn(array $f): Func => Func::fromArray($f), $data['functions'] ?? []),
            procedures: array_map(static fn(array $p): Procedure => Procedure::fromArray(
                $p,
            ), $data['procedures'] ?? []),
            domains: array_map(static fn(array $d): Domain => Domain::fromArray($d), $data['domains'] ?? []),
        );
    }

    public function hasSequence(string $name): bool
    {
        foreach ($this->sequences as $sequence) {
            if ($sequence->name === $name) {
                return true;
            }
        }

        return false;
    }

    public function hasTable(string $name): bool
    {
        foreach ($this->tables as $table) {
            if ($table->name === $name) {
                return true;
            }
        }

        return false;
    }

    public function merge(self $other): self
    {
        return new self(
            name: $this->name,
            tables: self::mergeByName($this->tables, $other->tables, static fn(Table $t): string => $t->name),
            sequences: self::mergeByName(
                $this->sequences,
                $other->sequences,
                static fn(Sequence $s): string => $s->name,
            ),
            views: self::mergeByName($this->views, $other->views, static fn(View $v): string => $v->name),
            materializedViews: self::mergeByName(
                $this->materializedViews,
                $other->materializedViews,
                static fn(MaterializedView $mv): string => $mv->name,
            ),
            functions: self::mergeByName($this->functions, $other->functions, static fn(Func $f): string => $f->name),
            procedures: self::mergeByName(
                $this->procedures,
                $other->procedures,
                static fn(Procedure $p): string => $p->name,
            ),
            domains: self::mergeByName($this->domains, $other->domains, static fn(Domain $d): string => $d->name),
        );
    }

    /**
     * @return SchemaShape
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'tables' => array_map(static fn(Table $t): array => $t->normalize(), $this->tables),
            'sequences' => array_map(static fn(Sequence $s): array => $s->normalize(), $this->sequences),
            'views' => array_map(static fn(View $v): array => $v->normalize(), $this->views),
            'materialized_views' => array_map(
                static fn(MaterializedView $mv): array => $mv->normalize(),
                $this->materializedViews,
            ),
            'functions' => array_map(static fn(Func $f): array => $f->normalize(), $this->functions),
            'procedures' => array_map(static fn(Procedure $p): array => $p->normalize(), $this->procedures),
            'domains' => array_map(static fn(Domain $d): array => $d->normalize(), $this->domains),
        ];
    }

    public function sequence(string $name): Sequence
    {
        foreach ($this->sequences as $sequence) {
            if ($sequence->name === $name) {
                return $sequence;
            }
        }

        throw new SchemaException(sprintf('Sequence "%s" not found in schema "%s".', $name, $this->name));
    }

    public function table(string $name): Table
    {
        foreach ($this->tables as $table) {
            if ($table->name === $name) {
                return $table;
            }
        }

        throw TableNotFoundException::inSchema($name, $this->name);
    }

    /**
     * @return list<string>
     */
    public function tableNames(): array
    {
        return array_map(static fn(Table $t): string => $t->name, $this->tables);
    }

    /**
     * @param ExecutionOrderStrategy<Table> $tableOrderStrategy
     * @param ExecutionOrderStrategy<View> $viewOrderStrategy
     * @param ExecutionOrderStrategy<MaterializedView> $materializedViewOrderStrategy
     *
     * @return list<Sql>
     */
    public function toSql(
        ExecutionOrderStrategy $tableOrderStrategy = new ForeignKeyDependencyOrder(),
        ExecutionOrderStrategy $viewOrderStrategy = new ViewDependencyOrder(new Parser()),
        ExecutionOrderStrategy $materializedViewOrderStrategy = new MaterializedViewDependencyOrder(new Parser()),
    ): array {
        $sqls = [];

        foreach ($this->domains as $domain) {
            $sqls[] = $domain->toSql();
        }

        foreach ($this->sequences as $seq) {
            $sqls[] = $seq->toSql();
        }

        foreach ($this->functions as $func) {
            $sql = $func->toSql();

            if ($sql !== null) {
                $sqls[] = $sql;
            }
        }

        foreach ($this->procedures as $proc) {
            $sql = $proc->toSql();

            if ($sql !== null) {
                $sqls[] = $sql;
            }
        }

        foreach ($tableOrderStrategy->order($this->tables) as $table) {
            $sqls = [...$sqls, ...$table->toSql()];
        }

        foreach ($viewOrderStrategy->order($this->views) as $view) {
            $sqls[] = $view->toSql();
        }

        foreach ($materializedViewOrderStrategy->order($this->materializedViews) as $mv) {
            $sqls = [...$sqls, ...$mv->toSql()];
        }

        return $sqls;
    }

    /**
     * @template T of Table|View|MaterializedView|Sequence|Func|Procedure|Domain
     *
     * @param list<T> $objects
     *
     * @return list<T>
     */
    private static function bind(array $objects, string $schemaName): array
    {
        $bound = [];

        foreach ($objects as $object) {
            if ($object->schema === null) {
                /** @var T $withSchema */
                $withSchema = $object->withSchema($schemaName);
                $bound[] = $withSchema;

                continue;
            }

            if ($object->schema !== $schemaName) {
                [$label, $dslFunction] = match (true) {
                    $object instanceof Table => ['Table', 'schema_table'],
                    $object instanceof View => ['View', 'schema_view'],
                    $object instanceof MaterializedView => ['Materialized view', 'schema_materialized_view'],
                    $object instanceof Sequence => ['Sequence', 'schema_sequence'],
                    $object instanceof Func => ['Function', 'schema_function'],
                    $object instanceof Procedure => ['Procedure', 'schema_procedure'],
                    default => ['Domain', 'schema_domain'],
                };

                throw new SchemaException(sprintf(
                    '%s "%s" declares schema "%s" but is placed in schema "%s"; pass schema: "%s" to %s() or move it.',
                    $label,
                    $object->name,
                    $object->schema,
                    $schemaName,
                    $schemaName,
                    $dslFunction,
                ));
            }

            $bound[] = $object;
        }

        return $bound;
    }

    /**
     * @template T
     *
     * @param list<T> $base
     * @param list<T> $override
     * @param callable(T): string $nameOf
     *
     * @return list<T>
     */
    private static function mergeByName(array $base, array $override, callable $nameOf): array
    {
        $indexed = [];

        foreach ($base as $item) {
            $indexed[$nameOf($item)] = $item;
        }

        foreach ($override as $item) {
            $indexed[$nameOf($item)] = $item;
        }

        return array_values($indexed);
    }
}
