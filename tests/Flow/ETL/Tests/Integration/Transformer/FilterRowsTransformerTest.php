<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\Filter\Filter\ValidValue;
use Flow\ETL\Transformer\FilterRowsTransformer;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Validator\Constraints\Email;
use Symfony\Component\Validator\Constraints\NotBlank;

final class FilterRowsTransformerTest extends TestCase
{
    public function test_symfony_filter_integration() : void
    {
        $rows = new Rows(
            Row::create(new Row\Entry\StringEntry('email', '')),
            Row::create(new Row\Entry\StringEntry('email', 'not_email')),
            Row::create(new Row\Entry\StringEntry('email', 'email@email.com'))
        );

        $transformer = new FilterRowsTransformer(
            new ValidValue(
                'email',
                new ValidValue\SymfonyValidator([
                    new NotBlank(),
                    new Email(),
                ])
            )
        );

        $this->assertSame(
            [
                [
                    'email' => 'email@email.com',
                ],
            ],
            $transformer->transform($rows)->toArray()
        );
    }
}
