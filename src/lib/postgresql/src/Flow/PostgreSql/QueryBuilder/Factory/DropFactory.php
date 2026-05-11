<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Factory;

use Flow\PostgreSql\QueryBuilder\Schema\Database\DropDatabaseBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Database\DropDatabaseFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Domain\DropDomainBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Domain\DropDomainFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\DropSequence\DropSequenceBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\DropSequence\DropSequenceFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\DropTable\DropTableBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\DropTable\DropTableFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Extension\DropExtensionBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Extension\DropExtensionFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Function\DropFunctionBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Function\DropFunctionFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Function\DropProcedureBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Function\DropProcedureFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Index\DropIndex\DropIndexBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Index\DropIndex\DropIndexFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Ownership\DropOwnedBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Ownership\DropOwnedFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Role\DropRoleBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Role\DropRoleFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Rule\DropRuleBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Rule\DropRuleOnStep;
use Flow\PostgreSql\QueryBuilder\Schema\Schema\DropSchemaBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Schema\DropSchemaFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\DropTriggerBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\DropTriggerOnStep;
use Flow\PostgreSql\QueryBuilder\Schema\Type\DropTypeBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Type\DropTypeFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\View\DropMaterializedView\DropMaterializedViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\DropMaterializedView\DropMatViewFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\View\DropView\DropViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\DropView\DropViewFinalStep;

final readonly class DropFactory
{
    public function database(string $name): DropDatabaseFinalStep
    {
        return DropDatabaseBuilder::create($name);
    }

    public function domain(string ...$domains): DropDomainFinalStep
    {
        return DropDomainBuilder::create(...$domains);
    }

    public function extension(string ...$extensions): DropExtensionFinalStep
    {
        return DropExtensionBuilder::create(...$extensions);
    }

    public function function(string $name): DropFunctionFinalStep
    {
        return DropFunctionBuilder::create($name);
    }

    public function index(string ...$indexes): DropIndexFinalStep
    {
        return DropIndexBuilder::create(...$indexes);
    }

    public function materializedView(string ...$views): DropMatViewFinalStep
    {
        return DropMaterializedViewBuilder::create(...$views);
    }

    public function owned(string ...$roles): DropOwnedFinalStep
    {
        return DropOwnedBuilder::create(...$roles);
    }

    public function procedure(string $name): DropProcedureFinalStep
    {
        return DropProcedureBuilder::create($name);
    }

    public function role(string ...$roles): DropRoleFinalStep
    {
        return DropRoleBuilder::create(...$roles);
    }

    public function rule(string $name): DropRuleOnStep
    {
        return DropRuleBuilder::create($name);
    }

    public function schema(string ...$schemas): DropSchemaFinalStep
    {
        return DropSchemaBuilder::create(...$schemas);
    }

    public function sequence(string ...$sequences): DropSequenceFinalStep
    {
        return DropSequenceBuilder::create()->sequence(...$sequences);
    }

    public function table(string ...$tables): DropTableFinalStep
    {
        return DropTableBuilder::create(...$tables);
    }

    public function trigger(string $name): DropTriggerOnStep
    {
        return DropTriggerBuilder::create($name);
    }

    public function type(string ...$types): DropTypeFinalStep
    {
        return DropTypeBuilder::create(...$types);
    }

    public function view(string ...$views): DropViewFinalStep
    {
        return DropViewBuilder::create(...$views);
    }
}
