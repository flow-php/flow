<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, df, equal, from_array, int_schema, join_on, row, rows, schema, str_schema, to_output};
use Flow\ETL\{DataFrame, DataFrameFactory, Extractor, FlowContext, Rows};
use Flow\ETL\Join\Join;
use Flow\ETL\Schema;

require __DIR__ . '/vendor/autoload.php';

$apiSchema = schema(int_schema('id'), str_schema('sku'));

$apiExtractor = new class($apiSchema) implements Extractor {
    public function __construct(private Schema $schema)
    {
    }

    public function extract(FlowContext $context): Generator
    {
        yield rows($this->schema, row(['id' => 1, 'sku' => 'PRODUCT01']), row(['id' => 2, 'sku' => 'PRODUCT02']));

        yield rows($this->schema, row(['id' => 10_001, 'sku' => 'PRODUCT10_001']));
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    public function withSchema(Schema $schema): static
    {
        $clone = clone $this;
        $clone->schema = $schema;

        return $clone;
    }
};

$dbDataFrameFactory = new class($apiSchema) implements DataFrameFactory {
    public function __construct(private Schema $schema)
    {
    }

    public function from(Rows $rows): DataFrame
    {
        // in real life this is one SQL query for $rows->reduceToArray('id')
        $known = [
            ['id' => 1, 'sku' => 'PRODUCT01'],
            ['id' => 2, 'sku' => 'PRODUCT02'],
        ];

        return df()->read(from_array($known, $this->schema));
    }
};

data_frame()
    ->extract($apiExtractor)
    // left_anti keeps the rows the database does not know about
    ->joinEach($dbDataFrameFactory, join_on(equal('id', 'id')), Join::left_anti)
    ->write(to_output(truncate: false))
    ->run();
