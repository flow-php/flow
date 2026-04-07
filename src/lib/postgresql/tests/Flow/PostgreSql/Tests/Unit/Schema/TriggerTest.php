<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use function Flow\PostgreSql\DSL\schema_trigger;
use Flow\PostgreSql\Schema\{TriggerEvent, TriggerTiming};

use PHPUnit\Framework\TestCase;

final class TriggerTest extends TestCase
{
    public function test_is_equal_for_identical_triggers() : void
    {
        $a = schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn', forEachRow: true, whenCondition: 'NEW.x > 0');
        $b = schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn', forEachRow: true, whenCondition: 'NEW.x > 0');

        self::assertTrue($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_events_differ() : void
    {
        $a = schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn');
        $b = schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::UPDATE], 'audit_fn');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_function_name_differs() : void
    {
        $a = schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'fn_a');
        $b = schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'fn_b');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_name_differs() : void
    {
        $a = schema_trigger('trg_a', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn');
        $b = schema_trigger('trg_b', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_table_name_differs() : void
    {
        $a = schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn');
        $b = schema_trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_timing_differs() : void
    {
        $a = schema_trigger('trg_audit', 'orders', TriggerTiming::BEFORE, [TriggerEvent::INSERT], 'audit_fn');
        $b = schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_structure_returns_false_when_for_each_row_differs() : void
    {
        $a = schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn', forEachRow: true);
        $b = schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn', forEachRow: false);

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_timing_differs() : void
    {
        $a = schema_trigger('trg_audit', 'orders', TriggerTiming::BEFORE, [TriggerEvent::INSERT], 'audit_fn');
        $b = schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn');

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_when_condition_differs() : void
    {
        $a = schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn', whenCondition: 'NEW.x > 0');
        $b = schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn', whenCondition: 'NEW.x > 100');

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_when_only_name_and_table_differ() : void
    {
        $a = schema_trigger('trg_a', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn', forEachRow: true, whenCondition: 'NEW.x > 0');
        $b = schema_trigger('trg_b', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn', forEachRow: true, whenCondition: 'NEW.x > 0');

        self::assertTrue($a->isEqualStructure($b));
    }

    public function test_trigger_construction() : void
    {
        $trigger = schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_function');

        self::assertSame('trg_audit', $trigger->name);
        self::assertSame('orders', $trigger->tableName);
        self::assertSame(TriggerTiming::AFTER, $trigger->timing);
        self::assertSame([TriggerEvent::INSERT], $trigger->events);
        self::assertSame('audit_function', $trigger->functionName);
        self::assertFalse($trigger->forEachRow);
        self::assertNull($trigger->whenCondition);
    }

    public function test_trigger_for_each_row() : void
    {
        $trigger = schema_trigger('trg_update', 'users', TriggerTiming::BEFORE, [TriggerEvent::UPDATE], 'update_fn', forEachRow: true);

        self::assertTrue($trigger->forEachRow);
    }

    public function test_trigger_with_when_condition() : void
    {
        $trigger = schema_trigger('trg_check', 'orders', TriggerTiming::BEFORE, [TriggerEvent::INSERT, TriggerEvent::UPDATE], 'check_fn', whenCondition: 'NEW.amount > 1000');

        self::assertSame('NEW.amount > 1000', $trigger->whenCondition);
        self::assertCount(2, $trigger->events);
    }
}
