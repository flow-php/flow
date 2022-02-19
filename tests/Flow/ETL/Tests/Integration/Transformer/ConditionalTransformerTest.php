<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\ArrayExpandTransformer;
use Flow\ETL\Transformer\ChainTransformer;
use Flow\ETL\Transformer\Condition\EntryExists;
use Flow\ETL\Transformer\Condition\Opposite;
use Flow\ETL\Transformer\Condition\ValidValue;
use Flow\ETL\Transformer\ConditionalTransformer;
use Flow\ETL\Transformer\StaticEntryTransformer;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Validator\Constraints\Email;
use Symfony\Component\Validator\Constraints\NotBlank;

final class ConditionalTransformerTest extends TestCase
{
    public function test_symfony_filter_integration() : void
    {
        $rows = new Rows(
            Row::create(new Row\Entry\StringEntry('email', '')),
            Row::create(new Row\Entry\StringEntry('email', 'not_email')),
            Row::create(new Row\Entry\StringEntry('email', 'email@email.com'))
        );

        $transformer = new ChainTransformer(
            new ConditionalTransformer(
                new ValidValue(
                    'email',
                    new ValidValue\SymfonyValidator([
                        new NotBlank(),
                        new Email(),
                    ])
                ),
                new StaticEntryTransformer(new Row\Entry\BooleanEntry('valid', true))
            ),
            new ConditionalTransformer(
                new Opposite(
                    new ValidValue(
                        'email',
                        new ValidValue\SymfonyValidator([
                            new NotBlank(),
                            new Email(),
                        ])
                    )
                ),
                new StaticEntryTransformer(new Row\Entry\BooleanEntry('valid', false))
            )
        );

        $this->assertSame(
            [
                [
                    'email' => '',
                    'valid' => false,
                ],
                [
                    'email' => 'not_email',
                    'valid' => false,
                ],
                [
                    'email' => 'email@email.com',
                    'valid' => true,
                ],
            ],
            $transformer->transform($rows)->toArray()
        );
    }

    public function test_returns_all_expanded_rows() : void
    {
        $conditionalTransformer = new ConditionalTransformer(
            new EntryExists('array_entry'),
            new ArrayExpandTransformer('array_entry', 'array_entry')
        );

        $rows = $conditionalTransformer->transform(
            new Rows(
                new Row(new Row\Entries(new Row\Entry\StringEntry('name', 'without array entry'))),
                new Row(new Row\Entries(
                    new Row\Entry\StringEntry('name', 'with array entry'),
                    new Row\Entry\ArrayEntry('array_entry', ['red', 'blue'])
                ))
            )
        );

        $this->assertEquals(3, $rows->count());
    }
}
