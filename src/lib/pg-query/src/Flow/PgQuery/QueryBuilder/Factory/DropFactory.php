<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Factory;

use Flow\PgQuery\QueryBuilder\Schema\Domain\{DropDomainBuilder, DropDomainFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\DropSequence\{DropSequenceBuilder, DropSequenceFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\DropTable\{DropTableBuilder, DropTableFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Extension\{DropExtensionBuilder, DropExtensionFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Function\{DropFunctionBuilder, DropFunctionFinalStep, DropProcedureBuilder, DropProcedureFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Index\DropIndex\{DropIndexBuilder, DropIndexFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Ownership\{DropOwnedBuilder, DropOwnedFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Role\{DropRoleBuilder, DropRoleFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Rule\{DropRuleBuilder, DropRuleOnStep};
use Flow\PgQuery\QueryBuilder\Schema\Schema\{DropSchemaBuilder, DropSchemaFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Trigger\{DropTriggerBuilder, DropTriggerOnStep};
use Flow\PgQuery\QueryBuilder\Schema\Type\{DropTypeBuilder, DropTypeFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\View\DropMaterializedView\{DropMatViewFinalStep, DropMaterializedViewBuilder};
use Flow\PgQuery\QueryBuilder\Schema\View\DropView\{DropViewBuilder, DropViewFinalStep};

final readonly class DropFactory
{
    public function domain(string ...$domains) : DropDomainFinalStep
    {
        return DropDomainBuilder::create(...$domains);
    }

    public function extension(string ...$extensions) : DropExtensionFinalStep
    {
        return DropExtensionBuilder::create(...$extensions);
    }

    public function function(string $name) : DropFunctionFinalStep
    {
        return DropFunctionBuilder::create($name);
    }

    public function index(string ...$indexes) : DropIndexFinalStep
    {
        return DropIndexBuilder::create(...$indexes);
    }

    public function materializedView(string ...$views) : DropMatViewFinalStep
    {
        return DropMaterializedViewBuilder::create(...$views);
    }

    public function owned(string ...$roles) : DropOwnedFinalStep
    {
        return DropOwnedBuilder::create(...$roles);
    }

    public function procedure(string $name) : DropProcedureFinalStep
    {
        return DropProcedureBuilder::create($name);
    }

    public function role(string ...$roles) : DropRoleFinalStep
    {
        return DropRoleBuilder::create(...$roles);
    }

    public function rule(string $name) : DropRuleOnStep
    {
        return DropRuleBuilder::create($name);
    }

    public function schema(string ...$schemas) : DropSchemaFinalStep
    {
        return DropSchemaBuilder::create(...$schemas);
    }

    public function sequence(string ...$sequences) : DropSequenceFinalStep
    {
        return DropSequenceBuilder::create()->sequence(...$sequences);
    }

    public function table(string ...$tables) : DropTableFinalStep
    {
        return DropTableBuilder::create(...$tables);
    }

    public function trigger(string $name) : DropTriggerOnStep
    {
        return DropTriggerBuilder::create($name);
    }

    public function type(string ...$types) : DropTypeFinalStep
    {
        return DropTypeBuilder::create(...$types);
    }

    public function view(string ...$views) : DropViewFinalStep
    {
        return DropViewBuilder::create(...$views);
    }
}
