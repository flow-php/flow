<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Function\ToMoney;
use Flow\ETL\Row;
use Money\Currency;
use Money\Money;
use PHPUnit\Framework\TestCase;

final class ToMoneyTest extends TestCase
{
    public function test_money_amount() : void
    {
        $row = Row::create(str_entry('a', '19.90'), str_entry('b', 'USD'));

        $this->assertEquals(
            new Money('1990', new Currency('USD')),
            (new ToMoney(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_money_amount_float() : void
    {
        $row = Row::create(float_entry('a', 19.90), str_entry('b', 'USD'));

        $this->assertEquals(
            new Money('1990', new Currency('USD')),
            (new ToMoney(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_money_amount_integer() : void
    {
        $row = Row::create(int_entry('a', 19), str_entry('b', 'USD'));

        $this->assertEquals(
            new Money('1900', new Currency('USD')),
            (new ToMoney(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_non_numeric_money_amount() : void
    {
        $row = Row::create(bool_entry('a', false), null_entry('b'));

        $this->assertNull(
            (new ToMoney(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_null_currency() : void
    {
        $row = Row::create(str_entry('a', '19.90'), null_entry('b'));

        $this->assertNull(
            (new ToMoney(ref('a'), ref('b')))->eval($row)
        );
    }
}
