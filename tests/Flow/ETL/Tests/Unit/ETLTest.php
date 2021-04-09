<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\ETL;
use Flow\ETL\Extractor;
use Flow\ETL\Filter\TrimRowToContainOnly;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\AddStampToStringEntryTransformer;
use Flow\ETL\Transformer;
use PHPUnit\Framework\TestCase;

final class ETLTest extends TestCase
{
    public function test_etl() : void
    {
        $extractor =  new class implements Extractor {
            /**
             * @return \Generator<int, Rows, mixed, void>
             */
            public function extract() : \Generator
            {
                yield new Rows(
                    Row::create(
                        new IntegerEntry('id', 101),
                        new BooleanEntry('deleted', false),
                        new DateEntry('expiration-date', new \DateTimeImmutable('2020-08-24')),
                        new NullEntry('phase')
                    ),
                    Row::create(
                        new IntegerEntry('id', 102),
                        new BooleanEntry('deleted', true),
                        new DateEntry('expiration-date', new \DateTimeImmutable('2020-08-25')),
                        new NullEntry('phase')
                    )
                );
            }
        };

        $addStampStringEntry = new class implements Transformer {
            public function transform(Rows $rows) : Rows
            {
                return $rows->map(
                    fn (Row $row) : Row => $row->set(new StringEntry('stamp', 'zero'))
                );
            }
        };

        $leaveOnlyIdAndStampEntry = new class implements Transformer {
            public function transform(Rows $rows) : Rows
            {
                return $rows->map(new TrimRowToContainOnly('id', 'stamp'));
            }
        };

        $loader = new class implements Loader {
            public array $result;

            public function load(Rows $rows) : void
            {
                $this->result = $rows->toArray();
            }
        };

        ETL::extract($extractor)
            ->transform($addStampStringEntry)
            ->transform(
                AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'one'),
                AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'two')
            )
            ->transform(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'three'))
            ->transform($leaveOnlyIdAndStampEntry)
            ->load($loader);

        $this->assertEquals(
            [
                [
                    'id' => 101,
                    'stamp' => 'zero:one:two:three',
                ],
                [
                    'id' => 102,
                    'stamp' => 'zero:one:two:three',
                ],
            ],
            $loader->result,
        );
    }
}
