<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use Avro\AvroDebug;
use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOSchemaMatchException;
use Avro\IO\AvroStringIO;
use Avro\IO\Binary\AvroIOBinaryDecoder;
use Avro\IO\Binary\AvroIOBinaryEncoder;
use Avro\IO\Exception\AvroIOException;
use Avro\Schema\AvroSchema;

require_once('test_helper.php');

class DatumIOTest extends PHPUnit\Framework\TestCase {

  /**
   * @dataProvider dataProvider
   * @param $schemaJson
   * @param $datum
   * @param $binary
   * @throws AvroException
   * @throws AvroIOException
   * @throws AvroIOSchemaMatchException
   * @throws AvroSchemaParseException
   * @throws \Avro\IO\AvroIOTypeException
   */
  function testDatumRoundTrip($schemaJson, $datum, $binary) {
    $schema = AvroSchema::parse($schemaJson);
    $written = new AvroStringIO();
    $encoder = new AvroIOBinaryEncoder($written);

    $schema->write($datum, $encoder);
    $output = strval($written);
    $this->assertEquals($binary, $output,
      sprintf("expected: %s\n  actual: %s",
        AvroDebug::asciiString($binary, 'hex'),
        AvroDebug::asciiString($output, 'hex'))
    );

    $read = new AvroStringIO($binary);
    $decoder = new AvroIOBinaryDecoder($read);
    $readDatum = $schema->read($decoder);
    $this->assertEquals($datum, $readDatum);
  }

  function dataProvider() {
    return [
      ['"null"', null, ''],

      ['"boolean"', true, "\001"],
      ['"boolean"', false, "\000"],

      ['"int"', (int) -2147483648, "\xFF\xFF\xFF\xFF\x0F"],
      ['"int"', -1, "\001"],
      ['"int"', 0, "\000"],
      ['"int"', 1, "\002"],
      ['"int"', 2147483647, "\xFE\xFF\xFF\xFF\x0F"],

      // array('"long"', (int) -9223372036854775808, "\001"),
      ['"long"', -1, "\001"],
      ['"long"', 0, "\000"],
      ['"long"', 1, "\002"],
      // array('"long"', 9223372036854775807, "\002")

      ['"float"', (float) -10.0, "\000\000 \301"],
      ['"float"', (float) -1.0, "\000\000\200\277"],
      ['"float"', (float) 0.0, "\000\000\000\000"],
      ['"float"', (float) 2.0, "\000\000\000@"],
      ['"float"', (float) 9.0, "\000\000\020A"],

      ['"double"', (double) -10.0, "\000\000\000\000\000\000$\300"],
      ['"double"', (double) -1.0, "\000\000\000\000\000\000\360\277"],
      ['"double"', (double) 0.0, "\000\000\000\000\000\000\000\000"],
      ['"double"', (double) 2.0, "\000\000\000\000\000\000\000@"],
      ['"double"', (double) 9.0, "\000\000\000\000\000\000\"@"],

      ['"string"', 'foo', "\x06foo"],
      ['"bytes"', "\x01\x02\x03", "\x06\x01\x02\x03"],

      [
        '{"type":"array","items":"int"}',
        [1, 2, 3],
        "\x06\x02\x04\x06\x00"
      ],
      [
        '{"type":"map","values":"int"}',
        ['foo' => 1, 'bar' => 2, 'baz' => 3],
        "\x06\x06foo\x02\x06bar\x04\x06baz\x06\x00"
      ],
      ['["null", "int"]', 1, "\x02\x02"],
      [
        '{"name":"fix","type":"fixed","size":3}',
        "\xAA\xBB\xCC",
        "\xAA\xBB\xCC"
      ],
      [
        '{"name":"enm","type":"enum","symbols":["A","B","C"]}',
        'B',
        "\x02"
      ],
      [
        '{"name":"rec","type":"record","fields":[{"name":"a","type":"int"},{"name":"b","type":"boolean"}]}',
        ['a' => 1, 'b' => false],
        "\x02\x00"
      ]
    ];
  }

  function defaultProvider() {
    return [
      ['"null"', 'null', null],
      ['"boolean"', 'true', true],
      ['"int"', '1', 1],
      ['"long"', '2000', 2000],
      ['"float"', '1.1', (float) 1.1],
      ['"double"', '200.2', (double) 200.2],
      ['"string"', '"quux"', 'quux'],
      ['"bytes"', '"\u00FF"', "\xC3\xBF"],
      [
        '{"type":"array","items":"int"}',
        '[5,4,3,2]',
        [5, 4, 3, 2]
      ],
      [
        '{"type":"map","values":"int"}',
        '{"a":9}',
        ['a' => 9]
      ],
      ['["int","string"]', '8', 8],
      [
        '{"name":"x","type":"enum","symbols":["A","V"]}',
        '"A"',
        'A'
      ],
      ['{"name":"x","type":"fixed","size":4}', '"\u00ff"', "\xC3\xBF"],
      [
        '{"name":"x","type":"record","fields":[{"name":"label","type":"int"}]}',
        '{"label":7}',
        ['label' => 7]
      ]
    ];
  }

  /**
   * @dataProvider defaultProvider
   * @param $fieldSchemaJson
   * @param $defaultJson
   * @param $defaultValue
   *
   * @throws AvroException
   * @throws AvroSchemaParseException
   * @throws AvroIOSchemaMatchException
   * @throws AvroIOException
   */
  function testFieldDefaultValue($fieldSchemaJson,
                                 $defaultJson, $defaultValue) {
    $writersSchemaJson = '{"name":"foo","type":"record","fields":[]}';
    $writersSchema = AvroSchema::parse($writersSchemaJson);

    $readersSchemaJson = sprintf(
      '{"name":"foo","type":"record","fields":[{"name":"f","type":%s,"default":%s}]}',
      $fieldSchemaJson, $defaultJson);
    $readersSchema = AvroSchema::parse($readersSchemaJson);

    $record = $writersSchema->read(new AvroIOBinaryDecoder(new AvroStringIO()), $readersSchema);
    if (array_key_exists('f', $record)) {
      $this->assertEquals($defaultValue, $record['f']);
    } else {
      $this->assertTrue(false, sprintf('expected field record[f]: %s', print_r($record, true)));
    }
  }

}
