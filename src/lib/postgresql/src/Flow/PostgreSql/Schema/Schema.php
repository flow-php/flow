<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Exception\SchemaException;
use Flow\PostgreSql\Schema\Exception\TableNotFoundException;

use function array_key_exists;
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
 * @import-type ExtensionShape from Extension
 *
 * @type SchemaShape = array{name: string, tables?: list<TableShape>, sequences?: list<SequenceShape>, views?: list<ViewShape>, materialized_views?: list<MaterializedViewShape>, functions?: list<FuncShape>, procedures?: list<ProcedureShape>, domains?: list<DomainShape>, extensions?: list<ExtensionShape>}
 */
final readonly class Schema
{
    /**
     * @param list<Table> $tables
     * @param list<Sequence> $sequences
     * @param list<View> $views
     * @param list<MaterializedView> $materializedViews
     * @param list<Func> $functions
     * @param list<Procedure> $procedures
     * @param list<Domain> $domains
     * @param list<Extension> $extensions
     */
    public function __construct(
        public string $name,
        public array $tables = [],
        public array $sequences = [],
        public array $views = [],
        public array $materializedViews = [],
        public array $functions = [],
        public array $procedures = [],
        public array $domains = [],
        public array $extensions = [],
    ) {}

    /**
     * @param SchemaShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(name: $data['name'], tables: array_map(static function (array $t) use ($data): Table {
            if (!array_key_exists('schema', $t)) {
                $t['schema'] = $data['name'];
            }

            return Table::fromArray($t);
        }, $data['tables'] ?? []), sequences: array_map(static fn(array $s): Sequence => Sequence::fromArray($s), $data['sequences'] ?? []), views: array_map(static fn(array $v): View => View::fromArray($v), $data['views'] ?? []), materializedViews: array_map(static fn(array $mv): MaterializedView => MaterializedView::fromArray($mv), $data['materialized_views'] ?? []), functions: array_map(static fn(array $f): Func => Func::fromArray($f), $data['functions'] ?? []), procedures: array_map(static fn(array $p): Procedure => Procedure::fromArray($p), $data['procedures'] ?? []), domains: array_map(static fn(array $d): Domain => Domain::fromArray($d), $data['domains'] ?? []), extensions: array_map(static fn(array $e): Extension => Extension::fromArray($e), $data['extensions'] ?? []));
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
            extensions: self::mergeByName(
                $this->extensions,
                $other->extensions,
                static fn(Extension $e): string => $e->name,
            ),
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
            'extensions' => array_map(static fn(Extension $e): array => $e->normalize(), $this->extensions),
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

        foreach ($this->extensions as $ext) {
            $sqls[] = $ext->toSql();
        }

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
