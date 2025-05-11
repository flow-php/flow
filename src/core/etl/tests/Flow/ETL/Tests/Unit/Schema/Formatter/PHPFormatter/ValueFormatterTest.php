<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Formatter\PHPFormatter;

use Flow\ETL\Schema\Formatter\PHPFormatter\ValueFormatter;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

final class ValueFormatterTest extends FlowTestCase
{
    #[TestWith(['value' => 'string', 'output' => '"string"'])]
    #[TestWith(['value' => 1, 'output' => '1'])]
    #[TestWith(['value' => 1.1, 'output' => '1.1'])]
    #[TestWith(['value' => true, 'output' => 'true'])]
    #[TestWith(['value' => false, 'output' => 'false'])]
    #[TestWith(['value' => ['a', 'b', 'c'], 'output' => '["a", "b", "c"]'])]
    #[TestWith(['value' => ['a' => 'b'], 'output' => '["a" => "b"]'])]
    #[TestWith(['value' => null, 'output' => 'null'])]
    public function test_format_values(mixed $value, string $output) : void
    {
        self::assertEquals(
            $output,
            (new ValueFormatter())->format($value)
        );
    }
}
