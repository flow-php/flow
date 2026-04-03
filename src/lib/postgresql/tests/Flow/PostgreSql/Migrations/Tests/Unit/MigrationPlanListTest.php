<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit;

use Flow\PostgreSql\Migrations\{Direction, Migration, MigrationContext, MigrationPlan, MigrationPlanList, Version};
use PHPUnit\Framework\TestCase;

final class MigrationPlanListTest extends TestCase
{
    public function stubMigration() : Migration
    {
        return new class implements Migration {
            public function migrate(MigrationContext $context) : void
            {
            }

            public function transactional() : bool
            {
                return true;
            }
        };
    }

    public function test_count() : void
    {
        $list = new MigrationPlanList(
            new MigrationPlan(Version::fromString('20260401120000'), $this->stubMigration(), null, Direction::UP),
            new MigrationPlan(Version::fromString('20260402120000'), $this->stubMigration(), null, Direction::UP),
        );

        self::assertCount(2, $list);
    }

    public function test_empty_list() : void
    {
        $list = new MigrationPlanList();

        self::assertCount(0, $list);
        self::assertTrue($list->isEmpty());
    }

    public function test_is_not_empty() : void
    {
        $list = new MigrationPlanList(
            new MigrationPlan(Version::fromString('20260401120000'), $this->stubMigration(), null, Direction::UP),
        );

        self::assertFalse($list->isEmpty());
    }

    public function test_iteration() : void
    {
        $plan1 = new MigrationPlan(Version::fromString('20260401120000'), $this->stubMigration(), null, Direction::UP);
        $plan2 = new MigrationPlan(Version::fromString('20260402120000'), $this->stubMigration(), null, Direction::DOWN);

        $list = new MigrationPlanList($plan1, $plan2);

        $items = [];

        foreach ($list as $plan) {
            $items[] = $plan;
        }

        self::assertSame([$plan1, $plan2], $items);
    }
}
