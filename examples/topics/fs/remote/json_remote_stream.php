<?php declare(strict_types=1);

if ('' !== \Phar::running(false)) {
    print 'This example cannot be run in PHAR, please use CLI approach.';

    exit(1);
}

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Json;
use Flow\ETL\Filesystem\AwsS3Stream;
use Flow\ETL\Filesystem\AzureBlobStream;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../../../bootstrap.php';

$dotenv = new Dotenv();
$dotenv->load(__DIR__ . '/../../.env');

$s3_client_option = [
    'client' => [
        'credentials' => [
            'key' => $_ENV['S3_KEY'],
            'secret' => $_ENV['S3_SECRET'],
        ],
        'region' => 'eu-west-2',
        'version' => 'latest',
    ],
    'bucket' => 'flow-php',
];

$blob_account = $_ENV['AZURE_BLOB_ACCOUNT'];
$blob_key = $_ENV['AZURE_BLOB_KEY'];

$azure_blob_connection_string = [
    'connection-string' => "DefaultEndpointsProtocol=https;AccountName={$blob_account};AccountKey={$blob_key}",
    'container' => 'flow-php',
];

AwsS3Stream::register();
AzureBlobStream::register();

(new Flow())
    ->read(Json::from(new Path('flow-aws-s3://dataset.json', $s3_client_option), 10))
    ->withEntry('row', ref('row')->unpack())
    ->withEntry('row.id', ref('row.id')->cast('integer'))
    ->withEntry('name', concat(ref('row.name'), lit(' '), ref('row.last name')))
    ->drop('row.last name')
    ->write(Json::to(new Path('flow-azure-blob://dataset_test.json', $azure_blob_connection_string)))
    ->run();
