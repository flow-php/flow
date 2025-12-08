<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Schema\Trigger;

use Flow\PgQuery\Protobuf\AST\CmdType;
use Flow\PgQuery\QueryBuilder\Schema\Rule\RuleEvent;
use Flow\PgQuery\QueryBuilder\Schema\Trigger\{TriggerEvent, TriggerLevel, TriggerTiming};
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

final class TriggerEnumsTest extends TestCase
{
    #[Test]
    public function test_rule_event_delete_maps_to_cmd_type() : void
    {
        self::assertSame(CmdType::CMD_DELETE, RuleEvent::DELETE->value);
    }

    #[Test]
    public function test_rule_event_insert_maps_to_cmd_type() : void
    {
        self::assertSame(CmdType::CMD_INSERT, RuleEvent::INSERT->value);
    }

    #[Test]
    public function test_rule_event_select_maps_to_cmd_type() : void
    {
        self::assertSame(CmdType::CMD_SELECT, RuleEvent::SELECT->value);
    }

    #[Test]
    public function test_rule_event_update_maps_to_cmd_type() : void
    {
        self::assertSame(CmdType::CMD_UPDATE, RuleEvent::UPDATE->value);
    }

    #[Test]
    public function test_trigger_event_delete_has_value_8() : void
    {
        self::assertSame(8, TriggerEvent::DELETE->value);
    }

    #[Test]
    public function test_trigger_event_insert_has_value_4() : void
    {
        self::assertSame(4, TriggerEvent::INSERT->value);
    }

    #[Test]
    public function test_trigger_event_truncate_has_value_32() : void
    {
        self::assertSame(32, TriggerEvent::TRUNCATE->value);
    }

    #[Test]
    public function test_trigger_event_update_has_value_16() : void
    {
        self::assertSame(16, TriggerEvent::UPDATE->value);
    }

    #[Test]
    public function test_trigger_events_can_be_combined_as_bitmask() : void
    {
        $combined = TriggerEvent::INSERT->value | TriggerEvent::UPDATE->value | TriggerEvent::DELETE->value;

        self::assertSame(28, $combined);
        self::assertTrue(($combined & TriggerEvent::INSERT->value) !== 0);
        self::assertTrue(($combined & TriggerEvent::UPDATE->value) !== 0);
        self::assertTrue(($combined & TriggerEvent::DELETE->value) !== 0);
        self::assertFalse(($combined & TriggerEvent::TRUNCATE->value) !== 0);
    }

    #[Test]
    public function test_trigger_level_row_returns_true() : void
    {
        self::assertTrue(TriggerLevel::ROW->toBool());
    }

    #[Test]
    public function test_trigger_level_statement_returns_false() : void
    {
        self::assertFalse(TriggerLevel::STATEMENT->toBool());
    }

    #[Test]
    public function test_trigger_timing_after_has_value_0() : void
    {
        self::assertSame(0, TriggerTiming::AFTER->value);
    }

    #[Test]
    public function test_trigger_timing_before_has_value_2() : void
    {
        self::assertSame(2, TriggerTiming::BEFORE->value);
    }

    #[Test]
    public function test_trigger_timing_instead_of_has_value_64() : void
    {
        self::assertSame(64, TriggerTiming::INSTEAD_OF->value);
    }
}
