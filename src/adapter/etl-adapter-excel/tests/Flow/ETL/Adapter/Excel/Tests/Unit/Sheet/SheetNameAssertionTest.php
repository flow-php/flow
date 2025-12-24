<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit\Sheet;

use Flow\ETL\Adapter\Excel\Sheet\SheetNameAssertion;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

final class SheetNameAssertionTest extends FlowTestCase
{
    #[TestWith(['Sheet1'])]
    #[TestWith(['Excel Sheet'])]
    #[TestWith(['My Data'])]
    #[TestWith(['Report_2024'])]
    public function test_assert_passes_for_valid_name(string $validName) : void
    {
        SheetNameAssertion::assert($validName);

        $this->addToAssertionCount(1);
    }

    #[TestWith([''])]
    #[TestWith(['This is veeeeeery long excel sheet name, longer than 32 characters'])]
    #[TestWith(['Sheet/Name'])]
    #[TestWith(['Sheet*Name'])]
    #[TestWith(['Sheet?Name'])]
    #[TestWith(['Sheet:Name'])]
    #[TestWith(['Sheet[Name'])]
    #[TestWith(['Sheet]Name'])]
    public function test_assert_throws_for_invalid_name(string $invalidName) : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sheet name must be a valid Excel sheet name');

        SheetNameAssertion::assert($invalidName);
    }

    #[TestWith(['', false])]
    #[TestWith(['This is veeeeeery long excel sheet name, longer than 32 characters', false])]
    #[TestWith(['Sheet/Name', false])]
    #[TestWith(['Sheet*Name', false])]
    #[TestWith(['Sheet?Name', false])]
    #[TestWith(['Sheet1', true])]
    #[TestWith(['Excel Sheet', true])]
    #[TestWith(['My Data', true])]
    public function test_is_valid(string $sheetName, bool $expectedResult) : void
    {
        self::assertSame($expectedResult, SheetNameAssertion::isValid($sheetName));
    }
}
