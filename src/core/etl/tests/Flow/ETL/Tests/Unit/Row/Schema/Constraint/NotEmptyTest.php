<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema\Constraint;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\json_object_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\object_entry;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\struct_type;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_map;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\Row\Schema\Constraint\NotEmpty;
use PHPUnit\Framework\TestCase;

final class NotEmptyTest extends TestCase
{
    public function test_not_empty_is_not_satisfied() : void
    {
        $constraint = new NotEmpty();

        $this->assertFalse($constraint->isSatisfiedBy(array_entry('e', [])));
        $this->assertFalse($constraint->isSatisfiedBy(json_entry('e', [])));
        $this->assertFalse($constraint->isSatisfiedBy(json_object_entry('e', [])));
        $this->assertFalse($constraint->isSatisfiedBy(list_entry('e', [], type_list(type_int()))));
    }

    public function test_not_empty_is_satisfied() : void
    {
        $constraint = new NotEmpty();

        $this->assertTrue($constraint->isSatisfiedBy(array_entry('e', [1])));
        $this->assertTrue($constraint->isSatisfiedBy(bool_entry('e', false)));
        $this->assertTrue($constraint->isSatisfiedBy(datetime_entry('e', new \DateTimeImmutable())));
        $this->assertTrue($constraint->isSatisfiedBy(object_entry('e', type_int())));
        $this->assertTrue($constraint->isSatisfiedBy(float_entry('e', 1.1)));
        $this->assertTrue($constraint->isSatisfiedBy(int_entry('e', 1)));
        $this->assertTrue($constraint->isSatisfiedBy(json_entry('e', [1, 2])));
        $this->assertTrue($constraint->isSatisfiedBy(list_entry('e', [1, 2], type_list(type_int()))));
        $this->assertTrue($constraint->isSatisfiedBy(null_entry('e')));
        $this->assertTrue($constraint->isSatisfiedBy(object_entry('e', new \SplFixedArray(2))));
        $this->assertTrue($constraint->isSatisfiedBy(str_entry('e', 'e')));
        $this->assertTrue($constraint->isSatisfiedBy(list_entry('list', [1, 2, 3], type_list(type_int()))));
        $this->assertTrue($constraint->isSatisfiedBy(map_entry('map', ['NEW', 'PENDING'], type_map(type_int(), type_string()))));
        $this->assertTrue($constraint->isSatisfiedBy(struct_entry('e', ['id' => 1], struct_type(struct_element('id', type_int())))));
    }
}
