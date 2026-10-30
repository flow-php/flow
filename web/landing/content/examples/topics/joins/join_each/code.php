<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{array_to_rows, data_frame, equal, from_array, int_schema, join_on, schema, str_schema, to_output};
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\ExtractorFactory;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Schema;

require __DIR__ . '/vendor/autoload.php';

$apiSchema = schema(int_schema('id'), str_schema('sku'));

$apiExtractor = new class($apiSchema) implements Extractor {
    public function __construct(private Schema $schema)
    {
    }

    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        yield array_to_rows([['id' => 1, 'sku' => 'PRODUCT01'], ['id' => 2, 'sku' => 'PRODUCT02']], $this->schema);

        yield array_to_rows([['id' => 10_001, 'sku' => 'PRODUCT10_001']], $this->schema);
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    public function statistics(): Statistics
    {
        return new Statistics();
    }

    public function withSchema(Schema $schema): static
    {
        return new static($schema);
    }
};

$dbExtractorFactory = new class($apiSchema) implements ExtractorFactory {
    public function __construct(private Schema $schema)
    {
    }

    public function from(Rows $rows): Extractor
    {
        // in real life this is one SQL query for $rows->reduceToArray('id')
        $known = [
            ['id' => 1, 'sku' => 'PRODUCT01'],
            ['id' => 2, 'sku' => 'PRODUCT02'],
        ];

        return from_array($known, $this->schema);
    }
};

data_frame()
    ->read($apiExtractor)
    // left_anti keeps the rows the database does not know about
    ->joinEach($dbExtractorFactory, join_on(equal('id', 'id')), JoinType::left_anti)
    ->write(to_output(truncate: false))
    ->run();
