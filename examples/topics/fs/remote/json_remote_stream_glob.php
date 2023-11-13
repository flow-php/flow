<?php declare(strict_types=1);

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    print "This example cannot be run in PHAR, please use CLI approach.\n";

    exit(1);
}

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Adapter\Filesystem\AwsS3Stream;
use Flow\ETL\Adapter\Filesystem\AzureBlobStream;
use Flow\ETL\DSL\CSV;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../../../../vendor/autoload.php';

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
    ->read(CSV::from(new Path('flow-aws-s3://nested/**/*.csv', $s3_client_option)))
    ->withEntry('id', ref('id')->cast('int'))
    ->withEntry('name', concat(ref('name'), lit(' '), ref('last name')))
    ->drop('last name')
    ->write(CSV::to(new Path('flow-azure-blob://output.csv', $azure_blob_connection_string)))
    ->run();
