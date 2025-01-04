<?php

declare(strict_types=1);

namespace Flow\ParquetViewer\Tests\Integration;

use Flow\ParquetViewer\Parquet;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\ApplicationTester;

final class ReadDataCommandTest extends TestCase
{
    public function test_read_data_command() : void
    {
        $application = new Parquet();
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $path = \realpath(__DIR__ . '/../../Fixtures/flow.parquet');

        $tester = new ApplicationTester($application);
        $tester->run([
            'command' => 'read:data',
            'file' => $path,
            '-t' => 20,
        ]);

        self::assertSame(0, $tester->getStatusCode());
        self::assertSame(
            <<<'OUTPUT'
+---------+------------+---------------------+-----------+-----------------+------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+
| boolean |      int32 |               int64 |     float |          double |          decimal |               string |                 date |             datetime |    list_of_datetimes |          map_of_ints |      list_of_strings |          struct_flat |
+---------+------------+---------------------+-----------+-----------------+------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+
|    true |  601197183 | 1252452733354460846 | 10.250000 |        0.360800 |       458.779999 | Et itaque beatae sun | 2023-08-16T00:00:00+ | 2023-07-18T01:27:45+ | [{"date":"2023-08-09 | {"a":15173348,"b":19 | ["Ea quia ut numquam | {"id":1,"name":"name |
|    true | 1898860246 |   80604967340828891 | 10.250000 |   327433.406250 |      3284.820068 | Et consequuntur maio | 2023-09-23T00:00:00+ | 2023-02-05T16:56:28+ | [{"date":"2023-09-30 | {"a":1533619681,"b": | ["Debitis fugiat com | {"id":2,"name":"name |
|   false | 1315048828 | 3129070533160172325 | 10.250000 |  2203746.750000 |    569329.187500 | Aut est optio earum  | 2023-05-30T00:00:00+ | 2023-07-15T05:24:48+ | [{"date":"2023-03-31 | {"a":1749103854,"b": | ["Non voluptates aut | {"id":3,"name":"name |
|   false | 1558719417 | 6878707872420020635 | 10.250000 |        1.693900 |  25955398.000000 | Dolorem magnam qui d | 2023-08-13T00:00:00+ | 2023-03-24T21:14:45+ | [{"date":"2023-01-09 | {"a":1720840905,"b": | ["Nihil quod perspic | {"id":4,"name":"name |
|    true | 1012067503 | 8967249410708780846 | 10.250000 |  1750536.875000 |         8.250000 | Aut velit alias enim | 2023-10-16T00:00:00+ | 2023-10-05T18:39:53+ | [{"date":"2023-01-01 | {"a":811892167,"b":9 | ["Voluptas eos quisq | {"id":5,"name":"name |
|    true |   28238480 | 3652472020703415644 | 10.250000 |    22556.656250 |     41735.050781 | Ipsam voluptatem ips | 2023-05-15T00:00:00+ | 2023-06-22T00:51:17+ | [{"date":"2023-05-11 | {"a":2124941571,"b": | ["Ut sed debitis del | {"id":6,"name":"name |
|    true | 1294233247 | 7357477648138228591 | 10.250000 |   237157.000000 | 185585584.000000 | Voluptas itaque cupi | 2023-01-09T00:00:00+ | 2023-08-09T21:26:24+ | [{"date":"2023-09-04 | {"a":429815988,"b":1 | ["Nihil modi volupta | {"id":7,"name":"name |
|   false | 1250913218 | 7983709812438780296 | 10.250000 | 25667990.000000 |   3079052.750000 | Minus autem est eos  | 2023-07-10T00:00:00+ | 2023-08-28T18:12:31+ | [{"date":"2023-02-03 | {"a":412214284,"b":7 | ["Consectetur nobis  | {"id":8,"name":"name |
|    true | 2105356816 | 8855288021139833847 | 10.250000 |        0.930070 |      2433.840088 | Fuga aut rerum et de | 2023-08-21T00:00:00+ | 2023-03-27T19:05:04+ | [{"date":"2023-02-07 | {"a":1268800792,"b": | ["Labore ratione sun | {"id":9,"name":"name |
|    true | 1314291690 | 7670636630642412419 | 10.250000 |       73.367706 |  20244592.000000 | Voluptate non sit am | 2023-10-07T00:00:00+ | 2023-09-15T08:41:32+ | [{"date":"2023-05-25 | {"a":929312481,"b":2 | ["Iusto quam et est  | {"id":10,"name":"nam |
+---------+------------+---------------------+-----------+-----------------+------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+
10 rows

OUTPUT,
            $tester->getDisplay()
        );
    }
}
