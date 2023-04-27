<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression\ToMoney;
use Money\Currency;
use Money\Money;
use PHPUnit\Framework\TestCase;

final class ToMoneyTest extends TestCase
{
    public function test_money_amount() : void
    {
        $row = Row::create(Entry::str('a', '19.90'), Entry::str('b', 'USD'));

        $this->assertEquals(
            new Money('1990', new Currency('USD')),
            (new ToMoney(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_money_amount_float() : void
    {
        $row = Row::create(Entry::float('a', 19.90), Entry::str('b', 'USD'));

        $this->assertEquals(
            new Money('1990', new Currency('USD')),
            (new ToMoney(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_money_amount_integer() : void
    {
        $row = Row::create(Entry::integer('a', 19), Entry::str('b', 'USD'));

        $this->assertEquals(
            new Money('1900', new Currency('USD')),
            (new ToMoney(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_non_numeric_money_amount() : void
    {
        $row = Row::create(Entry::bool('a', false), Entry::null('b'));

        $this->assertNull(
            (new ToMoney(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_null_currency() : void
    {
        $row = Row::create(Entry::str('a', '19.90'), Entry::null('b'));

        $this->assertNull(
            (new ToMoney(ref('a'), ref('b')))->eval($row)
        );
    }
}
