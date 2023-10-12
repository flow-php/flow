<?php

declare(strict_types=1);

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;

return new class implements Extractor {
    private static array $colors = ['red', 'green', 'blue'];

    private static array $countries = ['us', 'pl', 'gb', 'fr', 'de', 'it'];

    public function extract(FlowContext $context) : Generator
    {
        $rows = [];

        for ($i = 0; $i <= 2_000_000; $i++) {
            $rows[] = [
                'id' => $i,
                'name' => 'Name',
                'last name' => 'Last Name',
                'phone' => '123 123 123',
                't_shirt_color' => self::$colors[\array_rand(self::$colors)],
                'country_code' => self::$countries[\array_rand(self::$countries)],
            ];

            if (\count($rows) >= 100_000) {
                yield array_to_rows($rows);

                $rows = [];
            }
        }

        if ([] !== $rows) {
            yield array_to_rows(...$rows);
        }
    }
};
