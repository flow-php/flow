<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema\Constraint;

use Flow\ETL\DSL\Entry;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\Row\Schema\Constraint\NotEmpty;
use PHPUnit\Framework\TestCase;

final class NotEmptyTest extends TestCase
{
    public function test_not_empty_is_not_satisfied() : void
    {
        $constraint = new NotEmpty();

        $this->assertFalse($constraint->isSatisfiedBy(Entry::array('e', [])));
        $this->assertFalse($constraint->isSatisfiedBy(Entry::json('e', [])));
        $this->assertFalse($constraint->isSatisfiedBy(Entry::json_object('e', [])));
        $this->assertFalse($constraint->isSatisfiedBy(Entry::list_of_int('e', [])));
    }

    public function test_not_empty_is_satisfied() : void
    {
        $constraint = new NotEmpty();

        $this->assertTrue($constraint->isSatisfiedBy(Entry::array('e', [1])));
        $this->assertTrue($constraint->isSatisfiedBy(Entry::boolean('e', false)));
        $this->assertTrue($constraint->isSatisfiedBy(Entry::datetime('e', new \DateTimeImmutable())));
        $this->assertTrue($constraint->isSatisfiedBy(Entry::object('e', ScalarType::integer())));
        $this->assertTrue($constraint->isSatisfiedBy(Entry::float('e', 1.1)));
        $this->assertTrue($constraint->isSatisfiedBy(Entry::integer('e', 1)));
        $this->assertTrue($constraint->isSatisfiedBy(Entry::json('e', [1, 2])));
        $this->assertTrue($constraint->isSatisfiedBy(Entry::list_of_int('e', [1, 2])));
        $this->assertTrue($constraint->isSatisfiedBy(Entry::null('e')));
        $this->assertTrue($constraint->isSatisfiedBy(Entry::object('e', new \SplFixedArray(2))));
        $this->assertTrue($constraint->isSatisfiedBy(Entry::string('e', 'e')));
        $this->assertTrue($constraint->isSatisfiedBy(Entry::list_of_int('list', [1, 2, 3])));
        $this->assertTrue($constraint->isSatisfiedBy(Entry::map('map', ['NEW', 'PENDING'], new MapType(MapKey::integer(), MapValue::string()))));
        $this->assertTrue($constraint->isSatisfiedBy(Entry::structure('e', ['id' => 1], new StructureType(new StructureElement('id', ScalarType::integer())))));
    }
}
