<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Factory;

use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use Flow\PostgreSql\QueryBuilder\Schema\CreateSequence\CreateSequenceBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\CreateSequence\CreateSequenceOptionsStep;
use Flow\PostgreSql\QueryBuilder\Schema\CreateTable\CreateTableBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\CreateTable\CreateTableColumnsStep;
use Flow\PostgreSql\QueryBuilder\Schema\CreateTable\CreateTemporaryTableColumnsStep;
use Flow\PostgreSql\QueryBuilder\Schema\CreateTableAs\CreateTableAsBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\CreateTableAs\CreateTableAsFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Database\CreateDatabaseBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Database\CreateDatabaseOptionsStep;
use Flow\PostgreSql\QueryBuilder\Schema\Domain\CreateDomainBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Domain\CreateDomainTypeStep;
use Flow\PostgreSql\QueryBuilder\Schema\Extension\CreateExtensionBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Extension\CreateExtensionOptionsStep;
use Flow\PostgreSql\QueryBuilder\Schema\Function\CreateFunctionArgsStep;
use Flow\PostgreSql\QueryBuilder\Schema\Function\CreateFunctionBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Function\CreateProcedureArgsStep;
use Flow\PostgreSql\QueryBuilder\Schema\Function\CreateProcedureBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Index\CreateIndex\CreateIndexBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Index\CreateIndex\CreateIndexOnStep;
use Flow\PostgreSql\QueryBuilder\Schema\Role\CreateRoleBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Role\CreateRoleOptionsStep;
use Flow\PostgreSql\QueryBuilder\Schema\Rule\CreateRuleBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Rule\CreateRuleEventStep;
use Flow\PostgreSql\QueryBuilder\Schema\Schema\CreateSchemaBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Schema\CreateSchemaOptionsStep;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\CreateTriggerBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\CreateTriggerTimingStep;
use Flow\PostgreSql\QueryBuilder\Schema\Type\CreateCompositeTypeAttributesStep;
use Flow\PostgreSql\QueryBuilder\Schema\Type\CreateCompositeTypeBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Type\CreateEnumTypeBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Type\CreateEnumTypeLabelsStep;
use Flow\PostgreSql\QueryBuilder\Schema\Type\CreateRangeTypeBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Type\CreateRangeTypeSubtypeStep;
use Flow\PostgreSql\QueryBuilder\Schema\View\CreateMaterializedView\CreateMaterializedViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\CreateMaterializedView\CreateMatViewOptionsStep;
use Flow\PostgreSql\QueryBuilder\Schema\View\CreateView\CreateViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\CreateView\CreateViewOptionsStep;
use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;

final readonly class CreateFactory
{
    public function compositeType(string $name): CreateCompositeTypeAttributesStep
    {
        return CreateCompositeTypeBuilder::create($name);
    }

    public function database(string $name): CreateDatabaseOptionsStep
    {
        return CreateDatabaseBuilder::create($name);
    }

    public function domain(string $name): CreateDomainTypeStep
    {
        return CreateDomainBuilder::create($name);
    }

    public function enumType(string $name): CreateEnumTypeLabelsStep
    {
        return CreateEnumTypeBuilder::create($name);
    }

    public function extension(string $name): CreateExtensionOptionsStep
    {
        return CreateExtensionBuilder::create($name);
    }

    public function function(string $name): CreateFunctionArgsStep
    {
        return CreateFunctionBuilder::create($name);
    }

    public function index(string $name): CreateIndexOnStep
    {
        return CreateIndexBuilder::create($name);
    }

    public function materializedView(string $name, ?string $schema = null): CreateMatViewOptionsStep
    {
        if ($schema !== null) {
            return CreateMaterializedViewBuilder::create($name, $schema);
        }

        $identifier = QualifiedIdentifier::parse($name);

        return CreateMaterializedViewBuilder::create($identifier->name(), $identifier->schema());
    }

    public function procedure(string $name): CreateProcedureArgsStep
    {
        return CreateProcedureBuilder::create($name);
    }

    public function rangeType(string $name): CreateRangeTypeSubtypeStep
    {
        return CreateRangeTypeBuilder::create($name);
    }

    public function role(string $role): CreateRoleOptionsStep
    {
        return CreateRoleBuilder::create($role);
    }

    public function rule(string $name): CreateRuleEventStep
    {
        return CreateRuleBuilder::create($name);
    }

    public function schema(string $name): CreateSchemaOptionsStep
    {
        return CreateSchemaBuilder::create($name);
    }

    public function sequence(string $name, ?string $schema = null): CreateSequenceOptionsStep
    {
        return CreateSequenceBuilder::create()->sequence($name, $schema);
    }

    public function table(string $table, ?string $schema = null): CreateTableColumnsStep
    {
        if ($schema !== null) {
            return CreateTableBuilder::create($table, $schema);
        }

        $identifier = QualifiedIdentifier::parse($table);

        return CreateTableBuilder::create($identifier->name(), $identifier->schema());
    }

    public function tableAs(string $table, SelectFinalStep $query, ?string $schema = null): CreateTableAsFinalStep
    {
        if ($schema !== null) {
            return CreateTableAsBuilder::create($table, $query, $schema);
        }

        $identifier = QualifiedIdentifier::parse($table);

        return CreateTableAsBuilder::create($identifier->name(), $query, $identifier->schema());
    }

    public function temporarySequence(string $name, ?string $schema = null): CreateSequenceOptionsStep
    {
        return CreateSequenceBuilder::createTemporary()->sequence($name, $schema);
    }

    public function temporaryTable(string $table, ?string $schema = null): CreateTemporaryTableColumnsStep
    {
        if ($schema !== null) {
            return CreateTableBuilder::createTemporary($table, $schema);
        }

        $identifier = QualifiedIdentifier::parse($table);

        return CreateTableBuilder::createTemporary($identifier->name(), $identifier->schema());
    }

    public function trigger(string $name): CreateTriggerTimingStep
    {
        return CreateTriggerBuilder::create($name);
    }

    public function view(string $name, ?string $schema = null): CreateViewOptionsStep
    {
        if ($schema !== null) {
            return CreateViewBuilder::create($name, $schema);
        }

        $identifier = QualifiedIdentifier::parse($name);

        return CreateViewBuilder::create($identifier->name(), $identifier->schema());
    }
}
