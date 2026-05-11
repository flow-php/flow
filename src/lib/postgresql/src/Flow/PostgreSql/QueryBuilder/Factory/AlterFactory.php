<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Factory;

use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use Flow\PostgreSql\QueryBuilder\Schema\AlterSequence\AlterSequenceBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\AlterSequence\AlterSequenceOptionsStep;
use Flow\PostgreSql\QueryBuilder\Schema\AlterTable\AlterTableBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\AlterTable\AlterTableFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Domain\AlterDomainActionStep;
use Flow\PostgreSql\QueryBuilder\Schema\Domain\AlterDomainBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Extension\AlterExtensionActionStep;
use Flow\PostgreSql\QueryBuilder\Schema\Extension\AlterExtensionBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Function\AlterFunctionArgsStep;
use Flow\PostgreSql\QueryBuilder\Schema\Function\AlterFunctionBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Function\AlterProcedureArgsStep;
use Flow\PostgreSql\QueryBuilder\Schema\Function\AlterProcedureBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Index\AlterIndex\AlterIndexBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Index\AlterIndex\AlterIndexFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Role\AlterRoleActionStep;
use Flow\PostgreSql\QueryBuilder\Schema\Role\AlterRoleBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Schema\AlterSchemaActionStep;
use Flow\PostgreSql\QueryBuilder\Schema\Schema\AlterSchemaBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\AlterTriggerBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\AlterTriggerOnStep;
use Flow\PostgreSql\QueryBuilder\Schema\Type\AlterEnumTypeActionStep;
use Flow\PostgreSql\QueryBuilder\Schema\Type\AlterEnumTypeBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\AlterMaterializedView\AlterMaterializedViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\AlterMaterializedView\AlterMatViewActionStep;
use Flow\PostgreSql\QueryBuilder\Schema\View\AlterView\AlterViewActionStep;
use Flow\PostgreSql\QueryBuilder\Schema\View\AlterView\AlterViewBuilder;

final readonly class AlterFactory
{
    public function domain(string $name): AlterDomainActionStep
    {
        return AlterDomainBuilder::create($name);
    }

    public function enumType(string $name): AlterEnumTypeActionStep
    {
        return AlterEnumTypeBuilder::create($name);
    }

    public function extension(string $name): AlterExtensionActionStep
    {
        return AlterExtensionBuilder::create($name);
    }

    public function function(string $name): AlterFunctionArgsStep
    {
        return AlterFunctionBuilder::create($name);
    }

    public function index(string $name, ?string $schema = null): AlterIndexFinalStep
    {
        return AlterIndexBuilder::create($name, $schema);
    }

    public function materializedView(string $name, ?string $schema = null): AlterMatViewActionStep
    {
        return AlterMaterializedViewBuilder::create($name, $schema);
    }

    public function procedure(string $name): AlterProcedureArgsStep
    {
        return AlterProcedureBuilder::create($name);
    }

    public function role(string $role): AlterRoleActionStep
    {
        return AlterRoleBuilder::create($role);
    }

    public function schema(string $name): AlterSchemaActionStep
    {
        return AlterSchemaBuilder::create($name);
    }

    public function sequence(string $name, ?string $schema = null): AlterSequenceOptionsStep
    {
        return AlterSequenceBuilder::create()->sequence($name, $schema);
    }

    public function table(string $table, ?string $schema = null): AlterTableFinalStep
    {
        if ($schema !== null) {
            return AlterTableBuilder::create($table, $schema);
        }

        $identifier = QualifiedIdentifier::parse($table);

        return AlterTableBuilder::create($identifier->name(), $identifier->schema());
    }

    public function trigger(string $name): AlterTriggerOnStep
    {
        return AlterTriggerBuilder::create($name);
    }

    public function view(string $name, ?string $schema = null): AlterViewActionStep
    {
        return AlterViewBuilder::create($name, $schema);
    }
}
