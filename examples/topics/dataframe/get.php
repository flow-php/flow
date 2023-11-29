<?php

declare(strict_types=1);

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

$etl = (new Flow())
    ->read(
        from_rows(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('name', 'foo')),
                Row::create(int_entry('id', 2), str_entry('name', 'bar')),
                Row::create(int_entry('id', 3), str_entry('name', 'baz')),
                Row::create(int_entry('id', 4), str_entry('name', 'foo')),
                Row::create(int_entry('id', 5), str_entry('name', 'bar')),
                Row::create(int_entry('id', 6), str_entry('name', 'baz')),
            ),
        )
    );

foreach ($etl->getEachAsArray() as $rowData) {
    \var_dump($rowData);
}

// Output
/**
 * array(2) {
 *   ["id"]=>
 *   int(1)
 *   ["name"]=>
 *   string(3) "foo"
 * }
 * array(2) {
 *   ["id"]=>
 *   int(2)
 *   ["name"]=>
 *   string(3) "bar"
 * }
 * array(2) {
 *   ["id"]=>
 *   int(3)
 *   ["name"]=>
 *   string(3) "baz"
 * }
 * array(2) {
 *   ["id"]=>
 *   int(4)
 *   ["name"]=>
 *   string(3) "foo"
 * }
 * array(2) {
 *   ["id"]=>
 *   int(5)
 *   ["name"]=>
 *   string(3) "bar"
 * }
 * array(2) {
 *   ["id"]=>
 *   int(6)
 *   ["name"]=>
 *   string(3) "baz"
 * }.
 */
