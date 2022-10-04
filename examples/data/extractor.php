<?php

declare(strict_types=1);

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Rows;

return new class implements Extractor {
    private static array $colors = ['red', 'green', 'blue'];

    private static array $countries = ['us', 'pl', 'gb', 'fr', 'de', 'it'];

    public function extract(FlowContext $context) : Generator
    {
        $rows = [];

        for ($i = 0; $i <= 2_000_000; $i++) {
            $rows[] = Row::create(
                new ArrayEntry(
                    'row',
                    [
                        'id' => $i,
                        'name' => 'Name',
                        'last name' => 'Last Name',
                        'phone' => '123 123 123',
                        't_shirt_color' => self::$colors[\array_rand(self::$colors)],
                        'country_code' => self::$countries[\array_rand(self::$countries)],
                    ]
                ),
            );

            if (\count($rows) >= 100_000) {
                yield new Rows(...$rows);

                $rows = [];
            }
        }

        if (\count($rows) >= 0) {
            yield new Rows(...$rows);
        }
    }
};
