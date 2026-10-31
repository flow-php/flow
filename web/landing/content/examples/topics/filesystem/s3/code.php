<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\Filesystem\DSL\fstab;
use function Flow\ETL\Adapter\Parquet\{from_parquet, to_parquet};
use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\Adapter\XML\to_xml;
use function Flow\ETL\DSL\{data_frame, from_array, lit, overwrite, ref, to_output};
use function Flow\Filesystem\Bridge\AsyncAWS\DSL\{aws_s3_client, aws_s3_filesystem};
use function Flow\Filesystem\DSL\path;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/vendor/autoload.php';

$fs = fstab()->for('file');

if ($fs->status(path(__DIR__ . '/.env')) === null) {
    print 'Example skipped. Please create .env file with AWS S3 credentials.' . PHP_EOL;

    return;
}

$dotenv = new Dotenv();
$dotenv->load(__DIR__ . '/.env');

$awsBucket = $_ENV['AWS_S3_BUCKET'] ?? null;
$awsRegion = $_ENV['AWS_S3_REGION'] ?? null;
$awsKey = $_ENV['AWS_S3_KEY'] ?? null;
$awsSecret = $_ENV['AWS_S3_SECRET'] ?? null;

if ($awsBucket === null || $awsRegion === null || $awsKey === null || $awsSecret === null) {
    print 'Example skipped. Please set AWS_S3_BUCKET, AWS_S3_REGION, AWS_S3_KEY and AWS_S3_SECRET in the .env file.' . PHP_EOL;

    return;
}

$s3 = aws_s3_filesystem(
    $awsBucket,
    aws_s3_client([
        'region' => $awsRegion,
        'accessKeyId' => $awsKey,
        'accessKeySecret' => $awsSecret,
    ])
);

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'test'],
        ['id' => 2, 'name' => 'test'],
        ['id' => 3, 'name' => 'test'],
        ['id' => 4, 'name' => 'test'],
    ]))
    ->write(to_parquet(path('aws-s3://test.parquet'), filesystem: $s3)->saveMode(overwrite()))
    ->write(to_csv(path('aws-s3://test.csv'), filesystem: $s3)->saveMode(overwrite()))
    ->write(to_xml(path('aws-s3://test.xml'), filesystem: $s3)->saveMode(overwrite()))
    ->write(to_json(path('aws-s3://test.json'), filesystem: $s3)->saveMode(overwrite()))
    ->withColumn('line', ref('id')->concat(lit('_'), ref('name')))
    ->drop('id', 'name')
    ->write(to_text(path('aws-s3://test.txt'), filesystem: $s3)->saveMode(overwrite()))
    ->run();

data_frame()
    ->read(from_parquet(path('aws-s3://test.parquet'), filesystem: $s3))
    ->write(to_output(truncate: false))
    ->run();
