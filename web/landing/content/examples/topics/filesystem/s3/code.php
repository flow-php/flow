<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\Adapter\Parquet\{from_parquet, to_parquet};
use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\Adapter\XML\to_xml;
use function Flow\ETL\DSL\{config_builder, data_frame, from_array, lit, overwrite, ref, to_output};
use function Flow\Filesystem\Bridge\AsyncAWS\DSL\{aws_s3_client, aws_s3_filesystem};
use function Flow\Filesystem\DSL\path;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/vendor/autoload.php';

if (!\file_exists(__DIR__ . '/.env')) {
    print 'Example skipped. Please create .env file with AWS S3 credentials.' . PHP_EOL;

    return;
}

$dotenv = new Dotenv();
$dotenv->load(__DIR__ . '/.env');

$awsBucket = $_ENV['AWS_S3_BUCKET'];
$awsRegion = $_ENV['AWS_S3_REGION'];
$awsKey = $_ENV['AWS_S3_KEY'];
$awsSecret = $_ENV['AWS_S3_SECRET'];

if (!\is_string($awsBucket) || !\is_string($awsRegion) || !\is_string($awsKey) || !\is_string($awsSecret)) {
    print 'Example skipped. AWS credentials must be strings.' . PHP_EOL;

    return;
}

$config = config_builder()
    ->mount(
        aws_s3_filesystem(
            $awsBucket,
            aws_s3_client([
                'region' => $awsRegion,
                'accessKeyId' => $awsKey,
                'accessKeySecret' => $awsSecret,
            ])
        )
    );

data_frame($config)
    ->read(from_array([
        ['id' => 1, 'name' => 'test'],
        ['id' => 2, 'name' => 'test'],
        ['id' => 3, 'name' => 'test'],
        ['id' => 4, 'name' => 'test'],
    ]))
    ->saveMode(overwrite())
    ->write(to_parquet(path('aws-s3://test.parquet')))
    ->write(to_csv(path('aws-s3://test.csv')))
    ->write(to_xml(path('aws-s3://test.xml')))
    ->write(to_json(path('aws-s3://test.json')))
    ->withEntry('line', ref('id')->concat(lit('_'), ref('name')))
    ->drop('id', 'name')
    ->write(to_text(path('aws-s3://test.txt')))
    ->run();

data_frame($config)
    ->read(from_parquet(path('aws-s3://test.parquet')))
    ->write(to_output(truncate: false))
    ->run();
