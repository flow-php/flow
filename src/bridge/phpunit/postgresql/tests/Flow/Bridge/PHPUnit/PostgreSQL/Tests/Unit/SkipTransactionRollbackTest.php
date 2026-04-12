<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Tests\Unit;

use Flow\Bridge\PHPUnit\PostgreSQL\SkipTransactionRollback;
use Flow\Bridge\PHPUnit\PostgreSQL\Tests\Unit\Fixture\{ConcreteFromSkippedAbstractTestCase, NormalTestCase, SkippedMethodTestCase, SkippedTestCase};
use PHPUnit\Framework\TestCase;

final class SkipTransactionRollbackTest extends TestCase
{
    public function test_attribute_can_target_class() : void
    {
        $attribute = (new \ReflectionClass(SkipTransactionRollback::class))
            ->getAttributes(\Attribute::class)[0]
            ->newInstance();

        self::assertNotSame(0, $attribute->flags & \Attribute::TARGET_CLASS);
    }

    public function test_attribute_can_target_method() : void
    {
        $attribute = (new \ReflectionClass(SkipTransactionRollback::class))
            ->getAttributes(\Attribute::class)[0]
            ->newInstance();

        self::assertNotSame(0, $attribute->flags & \Attribute::TARGET_METHOD);
    }

    public function test_attribute_detected_on_class() : void
    {
        self::assertNotEmpty(
            (new \ReflectionClass(SkippedTestCase::class))
                ->getAttributes(SkipTransactionRollback::class)
        );
    }

    public function test_attribute_detected_on_method() : void
    {
        self::assertNotEmpty(
            (new \ReflectionClass(SkippedMethodTestCase::class))
                ->getMethod('test_something')
                ->getAttributes(SkipTransactionRollback::class)
        );
    }

    public function test_attribute_detected_on_parent_abstract_class() : void
    {
        $reflectionClass = new \ReflectionClass(ConcreteFromSkippedAbstractTestCase::class);
        $parentClass = $reflectionClass->getParentClass();

        self::assertNotFalse($parentClass);
        self::assertNotEmpty($parentClass->getAttributes(SkipTransactionRollback::class));
    }

    public function test_attribute_not_present_on_normal_class() : void
    {
        self::assertEmpty(
            (new \ReflectionClass(NormalTestCase::class))
                ->getAttributes(SkipTransactionRollback::class)
        );
    }
}
