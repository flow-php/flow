<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Factory;

use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use Flow\PostgreSql\QueryBuilder\Schema\AlterSequence\{AlterSequenceBuilder, AlterSequenceOptionsStep};
use Flow\PostgreSql\QueryBuilder\Schema\AlterTable\{AlterTableBuilder, AlterTableFinalStep};
use Flow\PostgreSql\QueryBuilder\Schema\Domain\{AlterDomainActionStep, AlterDomainBuilder};
use Flow\PostgreSql\QueryBuilder\Schema\Extension\{AlterExtensionActionStep, AlterExtensionBuilder};
use Flow\PostgreSql\QueryBuilder\Schema\Function\{AlterFunctionArgsStep, AlterFunctionBuilder, AlterProcedureArgsStep, AlterProcedureBuilder};
use Flow\PostgreSql\QueryBuilder\Schema\Index\AlterIndex\{AlterIndexBuilder, AlterIndexFinalStep};
use Flow\PostgreSql\QueryBuilder\Schema\Role\{AlterRoleActionStep, AlterRoleBuilder};
use Flow\PostgreSql\QueryBuilder\Schema\Schema\{AlterSchemaActionStep, AlterSchemaBuilder};
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\{AlterTriggerBuilder, AlterTriggerOnStep};
use Flow\PostgreSql\QueryBuilder\Schema\Type\{AlterEnumTypeActionStep, AlterEnumTypeBuilder};
use Flow\PostgreSql\QueryBuilder\Schema\View\AlterMaterializedView\{AlterMatViewActionStep, AlterMaterializedViewBuilder};
use Flow\PostgreSql\QueryBuilder\Schema\View\AlterView\{AlterViewActionStep, AlterViewBuilder};

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
