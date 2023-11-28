<?php

declare(strict_types=1);

use function Flow\ETL\DSL\from_rows;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

$etl = (new Flow())
    ->read(
        from_rows(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'foo')),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'bar')),
                Row::create(Entry::integer('id', 3), Entry::string('name', 'baz')),
                Row::create(Entry::integer('id', 4), Entry::string('name', 'foo')),
                Row::create(Entry::integer('id', 5), Entry::string('name', 'bar')),
                Row::create(Entry::integer('id', 6), Entry::string('name', 'baz')),
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
