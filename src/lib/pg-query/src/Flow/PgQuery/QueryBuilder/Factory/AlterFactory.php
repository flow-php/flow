<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Factory;

use Flow\PgQuery\QueryBuilder\QualifiedIdentifier;
use Flow\PgQuery\QueryBuilder\Schema\AlterSequence\{AlterSequenceBuilder, AlterSequenceOptionsStep};
use Flow\PgQuery\QueryBuilder\Schema\AlterTable\{AlterTableBuilder, AlterTableFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Domain\{AlterDomainActionStep, AlterDomainBuilder};
use Flow\PgQuery\QueryBuilder\Schema\Extension\{AlterExtensionActionStep, AlterExtensionBuilder};
use Flow\PgQuery\QueryBuilder\Schema\Function\{AlterFunctionArgsStep, AlterFunctionBuilder, AlterProcedureArgsStep, AlterProcedureBuilder};
use Flow\PgQuery\QueryBuilder\Schema\Index\AlterIndex\{AlterIndexBuilder, AlterIndexFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Role\{AlterRoleActionStep, AlterRoleBuilder};
use Flow\PgQuery\QueryBuilder\Schema\Schema\{AlterSchemaActionStep, AlterSchemaBuilder};
use Flow\PgQuery\QueryBuilder\Schema\Trigger\{AlterTriggerBuilder, AlterTriggerOnStep};
use Flow\PgQuery\QueryBuilder\Schema\Type\{AlterEnumTypeActionStep, AlterEnumTypeBuilder};
use Flow\PgQuery\QueryBuilder\Schema\View\AlterMaterializedView\{AlterMatViewActionStep, AlterMaterializedViewBuilder};
use Flow\PgQuery\QueryBuilder\Schema\View\AlterView\{AlterViewActionStep, AlterViewBuilder};

final readonly class AlterFactory
{
    public function domain(string $name) : AlterDomainActionStep
    {
        return AlterDomainBuilder::create($name);
    }

    public function enumType(string $name) : AlterEnumTypeActionStep
    {
        return AlterEnumTypeBuilder::create($name);
    }

    public function extension(string $name) : AlterExtensionActionStep
    {
        return AlterExtensionBuilder::create($name);
    }

    public function function(string $name) : AlterFunctionArgsStep
    {
        return AlterFunctionBuilder::create($name);
    }

    public function index(string $name, ?string $schema = null) : AlterIndexFinalStep
    {
        return AlterIndexBuilder::create($name, $schema);
    }

    public function materializedView(string $name, ?string $schema = null) : AlterMatViewActionStep
    {
        return AlterMaterializedViewBuilder::create($name, $schema);
    }

    public function procedure(string $name) : AlterProcedureArgsStep
    {
        return AlterProcedureBuilder::create($name);
    }

    public function role(string $role) : AlterRoleActionStep
    {
        return AlterRoleBuilder::create($role);
    }

    public function schema(string $name) : AlterSchemaActionStep
    {
        return AlterSchemaBuilder::create($name);
    }

    public function sequence(string $name, ?string $schema = null) : AlterSequenceOptionsStep
    {
        return AlterSequenceBuilder::create()->sequence($name, $schema);
    }

    public function table(string $table, ?string $schema = null) : AlterTableFinalStep
    {
        if ($schema !== null) {
            return AlterTableBuilder::create($table, $schema);
        }

        $identifier = QualifiedIdentifier::parse($table);

        return AlterTableBuilder::create($identifier->name(), $identifier->schema());
    }

    public function trigger(string $name) : AlterTriggerOnStep
    {
        return AlterTriggerBuilder::create($name);
    }

    public function view(string $name, ?string $schema = null) : AlterViewActionStep
    {
        return AlterViewBuilder::create($name, $schema);
    }
}
