<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class DataFrameJsonTest extends IntegrationTestCase
{
    public function test_building_data_frame_from_json() : void
    {
        $df = DataFrame::fromJson(
            <<<'JSON'
[
  {
    "function" : "data_frame",
    "call": {
      "method": "read",
      "args": [
        {
          "function": "from_array",
          "args": [
            [{"id": 1, "name": "Norbert"},{"id": 2, "name": "Michal"}]
          ]
        }
      ],
      "call": {
        "method": "withEntry",
        "args": ["active", {"function": "lit", "args":  [true]}]
      }
    }
  }
]
JSON
        );

        $this->assertSame(
            [
                ['id' => 1, 'name' => 'Norbert', 'active' => true],
                ['id' => 2, 'name' => 'Michal', 'active' => true],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_building_data_frame_from_json_with_forbidden_method_call() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Method \"run\" from class \"Flow\ETL\DataFrame\" is excluded from DSL.");

        DataFrame::fromJson(
            <<<'JSON'
[
  {
    "function" : "data_frame",
    "call": {
      "method": "read",
      "args": [
        {
          "function": "from_array",
          "args": [
            [{"id": 1, "name": "Norbert"},{"id": 2, "name": "Michal"}]
          ]
        }
      ],
      "call": {
        "method": "withEntry",
        "args": ["active", {"function": "lit", "args":  [true]}],
        "call": {
          "method": "run"
        }
      }
    }
  }
]
JSON
        );
    }

    public function test_building_dataframe_with_two_entry_points() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid JSON, please make sure that there is only one data_frame function');

        DataFrame::fromJson(
            <<<'JSON'
[
  {
    "function" : "data_frame",
    "call": {
      "method": "read",
      "args": [
        {
          "function": "from_array",
          "args": [
            [{"id": 1, "name": "Norbert"},{"id": 2, "name": "Michal"}]
          ]
        }
      ],
      "call": {
        "method": "withEntry",
        "args": ["active", {"function": "lit", "args":  [true]}]
      }
    }
  },
  {
    "function" : "data_frame",
    "call": {
      "method": "read",
      "args": [
        {
          "function": "from_array",
          "args": [
            [{"id": 1, "name": "Norbert"},{"id": 2, "name": "Michal"}]
          ]
        }
      ],
      "call": {
        "method": "withEntry",
        "args": ["active", {"function": "lit", "args":  [false]}]
      }
    }
  }
]
JSON
        );
    }

    public function test_building_dataframe_with_zero_entry_points() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Definition must have at least one function: [{"function":"name","args":[]}]');

        DataFrame::fromJson('[]');
    }
}
