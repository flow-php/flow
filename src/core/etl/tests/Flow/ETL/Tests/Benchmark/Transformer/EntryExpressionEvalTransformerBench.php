<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Benchmark\Transformer;

use function Flow\ETL\DSL\ref;
use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\EntryExpressionEvalTransformer;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(5)]
final class EntryExpressionEvalTransformerBench
{
    #[Revs(1000)]
    public function bench_transform_json_row() : void
    {
        (new EntryExpressionEvalTransformer('decoded', ref('json')->jsonDecode()))
            ->transform(
                new Rows(Row::create(Entry::json('json', ['some' => 'field', 'boolean' => false]))),
                new FlowContext(Config::default())
            );
    }

    #[Revs(1000)]
    public function bench_transform_string_row() : void
    {
        (new EntryExpressionEvalTransformer('string', ref('string')->upper()))
            ->transform(
                new Rows(Row::create(Entry::string('string', 'string'))),
                new FlowContext(Config::default())
            );
    }

    #[Revs(1000)]
    public function bench_transform_xml_row() : void
    {
        (new EntryExpressionEvalTransformer('xpath', ref('xml')->xpath('/root/foo')))
            ->transform(
                new Rows(Row::create(Entry::xml('xml', '<root><foo baz="buz">bar</foo><foo>baz</foo></root>'))),
                new FlowContext(Config::default())
            );
    }
}
