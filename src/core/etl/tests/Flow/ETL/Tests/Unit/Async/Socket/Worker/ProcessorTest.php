<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Async\Socket\Worker;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Async\Socket\Worker\Processor;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

final class ProcessorTest extends TestCase
{
    public function test_processing_rows_with_given_pipeline() : void
    {
        $processor = new Processor('id', new NullLogger());

        $processor->setPipes(new Pipes([
            new Transformer\EntryExpressionEvalTransformer(
                'full_name',
                concat(ref('name'), lit(' '), ref('last_name'))
            ),
            new Transformer\RemoveEntriesTransformer('name', 'last_name'),
        ]));

        $rows = $processor->process(
            new Rows(
                Row::create(
                    new Row\Entry\StringEntry('name', 'Johny'),
                    new Row\Entry\StringEntry('last_name', 'Bravo')
                )
            )
        );

        $this->assertSame(
            [
                ['full_name' => 'Johny Bravo'],
            ],
            $rows->toArray()
        );
    }
}
